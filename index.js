const Bluebird = require('Bluebird');
const _ = require('lodash');
const request = require('request');
const makeEta = require('simple-eta');
const { Command } = require('commander');

const program = new Command();
program.version('0.0.1');
program.option('-r, --regex <regex...>', 'The regex to split indexes and balance across sub group of indexes', ['([0-9]{4}(?:\\.|-)[0-9]{2}(?:\\.|-)[0-9]{2})'])
	.option('-h, --host <host>', 'The ES cluster to work on', 'http://127.0.0.1:9200')
	.option('-p, --pass <pass>', 'How much we should make to ensure balancing?', '5')
	.option('-m, --max-move-per-node <nbMaxMove>', 'How much move in parrallal are allowed from / to the same node', '2')
	.option('-t, --max-move-in-total <nbMaxMove>', 'How much move in total to make. 0 for unlimited', '0')
	.option('-c, --commit', 'Commit change to server')	
	.option('-s, --show-command', 'Show command to be run')
	.option('-d, --debug', 'output extra debugging')
	.option('-f, --filter <filter>', 'Filter', '')
	.parse();

const options = program.opts();
const {regex, pass, maxMovePerNode, maxMoveInTotal, host} = options;
let nbMove = 0;

// Allow of not rebalancing. If this automatic operation happen in the cluster, then our current state on which we work on is not accurate anymore.
const allowRebalancing = allow => requestPromise(`${host}/_cluster/settings`,
	{
		method: "PUT",
		json: true,
		gzip: true,
		body: {
			"transient" : {
				"cluster.routing.rebalance.enable" : (allow)?'all':'none'
			}
		}
	});

const requestPromise = (url, params) => Bluebird.fromCallback(callback => request(url, params, (err, res, body) => callback(err, [res, body])))

// Get data from the cluster
const getData = async () => {
	let [, body] = await requestPromise(`${host}/_cat/shards?bytes=mb&format=json`, {json: true, gzip: true});

	_.forEach(body, (index) => {
		index.originalNode = index.node
		index.docs = parseInt(index.docs, 10) || 0;
		index.store = parseInt(index.store, 10) || 0;
	});

	return body;
}

const updateProgress = async () => {
	let result = {};
	const [, body] = await requestPromise(`${host}/_cat/shards?bytes=mb&h=state,node&format=json`, {json: true, gzip: true});
	
	_.filter(body, i => i.state == 'RELOCATING' && i.node != "undefined").map(i => {
		const data = i.node.split(' ');
		increaseProgress(result, data[0]);
		increaseProgress(result, data[4]);
	});

	return result;
}

const regul = (subIndexes, type) => {
	const criteria = (type == 'count')?() => (1):(index) => (index.store);
	const sortCriteria = (type == 'count')?(index) => (index.store):(index) => (-index.store);

	const indexByNode = _.groupBy(subIndexes, 'node');
	const sumIndexed = _.mapValues(indexByNode, indexes => _.sumBy(indexes, criteria));
	const allNodes = Object.keys(indexByNode);
	let avg = _.sum(Object.values(sumIndexed)) / allNodes.length;

	let indexesToOffers = _.sortBy(subIndexes, sortCriteria);

	for (const nodeName of _.sortBy(allNodes, k => sumIndexed[k])) {
		let delta = avg - sumIndexed[nodeName];
		if (delta <= -1) continue;

		indexesToOffers = _.filter(indexesToOffers, indexeToOffer => {
			if (indexeToOffer.node == nodeName) return true;

			const d = criteria(indexeToOffer);
			if (d - delta >= 1) return true;
			if (d === 0) return false;

			if (sumIndexed[indexeToOffer.node] - d < avg) return true; // Will help no one
			if (_.findIndex(subIndexes, {node: nodeName, index: indexeToOffer.index, shard: indexeToOffer.shard}) != -1) return true;

			sumIndexed[indexeToOffer.node] -= d;
			indexeToOffer.node = nodeName;
			
			delta -= d;
			return false;
		});
	}
}

// Try to find spot by first regul size to have all node with same index count, then swap index to find an equilibrium in size
const findBetterSpot = (indexes) =>
	_.forOwn(indexes, (subIndexes) => {
		regul(subIndexes, 'size');
		regul(subIndexes, 'count');
	});

const increaseProgress = (result, node) => {
	result[node] = (result[node] || 0) + 1;
}

const applyAllMoves = async (moveToDo) => {
	const originalSize = remain(moveToDo);
	const eta = makeEta({ min: 0, max: originalSize, historyTimeConstant: 20 });
	
	while (moveToDo.length > 0 && (maxMoveInTotal == 0 || nbMove < maxMoveInTotal)) {
		const remainSize = remain(moveToDo);
		eta.report(originalSize - remainSize);
		console.log(`Remaining move: ${moveToDo.length} ${remainSize} Gb. ETA: ${Math.round(eta.estimate())}s`);
		moveToDo = await applyMoveWithSwitch(moveToDo);
	}
}

const applyMoveWithSwitch = async (moveToDo) => {
	let moveInProgress = await updateProgress();
	let commands = [];

	const apply1Move = (move) => {
		options.debug && console.log(`Ask to move ${move.index}#${move.shard} from ${move.originalNode} => ${move.node} for ${move.store}mb`);
		nbMove++;
		increaseProgress(moveInProgress, move.originalNode);
		increaseProgress(moveInProgress, move.node);
		commands.push({"move": {"index": move.index, "shard": move.shard, "from_node": move.originalNode, "to_node": move.node}});
		
		move.originalNode = move.node;
	}

	const ignoredMove = _.filter(moveToDo, (move) => {
		let {originalNode: from, node: to} = move;
		let reverse;

		if (maxMoveInTotal != 0 && nbMove > maxMoveInTotal) return true;

		if (moveInProgress[from] >= maxMovePerNode ||  moveInProgress[to] >= maxMovePerNode) {
			return true;
		}

		if (move.originalNode == move.node) {
			return false;
		}
		
		apply1Move(move);

		while (from != to) {
			reverse = _.find(moveToDo, subMove => subMove.originalNode != subMove.node && subMove.originalNode == to && subMove.node == from) // Find exact reverse
				|| _.find(moveToDo, subMove => subMove.originalNode != subMove.node && subMove.originalNode == to); // Find another move linked to the current one

			if (reverse == null) {
				break;
			}
			to = reverse.node;
			apply1Move(reverse);
		}
	});

	const [res, body] = await requestPromise(`${host}/_cluster/reroute`, {method: "POST", json: true, gzip: true, body: {"commands": commands}});
	if (body.status == 400) {
		// We get error. Move was not done, let's just reinsert it for later.
		console.log('Error, retry later', JSON.stringify(body));
	}

	return ignoredMove;
}

const remain = (moveToDo) => Math.round(_.sumBy(_.filter(moveToDo, 'store'), 'store')/1024);

const statsByType = (indexes, type) => {
	const criteria = (type == 'count')?() => (1):(index) => (index.store)
	const indexByNode = _.groupBy(indexes, 'node');
	const avgScore = _.sumBy(indexes, criteria) / Object.keys(indexByNode).length;

	return [
		Math.round(avgScore * 100) / 100, 
		_.mapValues(indexByNode, indexes => {
			const sum = _.sumBy(indexes, criteria);
			return [Math.round(sum/avgScore * 10000) / 100, sum];
		})
	];
}

const stats = indexes => ['count', 'size'].forEach(type => console.log(`Indexes ${type} by node`, statsByType(indexes, type)))

const showAllMoves = moveToDo => {
	console.log(`Move to do: ${moveToDo.length} ${remain(moveToDo)} Gb.`);
	for (const move of moveToDo) {
		console.log(`Will move ${move.index}#${move.shard} from ${move.originalNode} to ${move.node} ${move.store}Mb`);
	}
}

(async () => {
	// We don't want any move during our balancing
	await allowRebalancing(false);

	const filterRegex = new RegExp(options.filter);
	const groupRegex = regex.map (r => new RegExp(r));

	for (let i = 0 ; i < pass ; i++) {
		while (true) {
			const moveInProgress = await updateProgress();
			if (_.sum(Object.values(moveInProgress)) == 0) break;
			console.log('RELOCATING are in progress. We need to wait.');
			await Bluebird.delay(500);
		}

		console.log("\n---\nStart new pass");

		const indexes = _.filter(await getData(), i => filterRegex.test(i.index));
		indexesKeyed = _.groupBy(indexes, index => groupRegex.map(r => (index.index.match(r) || []).splice(1).join('##')).join('##'));
		
		options.debug && stats(indexes);
		findBetterSpot(indexesKeyed);

		// Unduplicate useless move (for exemple A.primary move from 1 => 2 and A.replica move from 2 => 3. In this case let's just move A.primary from 1 => 3 and let A.replica stay on 2)
		_.chain(indexes)
			.filter(i=>i.node != i.originalNode)
			.groupBy((i) => `${i.index}__${i.shard}`)
			.filter(g => g.length > 1)
			.forEach(g => {
				const from = _.map(g, i => i.originalNode);
				const to = _.map(g, i => i.node);
				
				const intersect = from.filter(value => to.includes(value));
				if (intersect.length == 0) return;
				for (const node of intersect) {
					const fromIndex = _.find(g, i => i.originalNode == node);
					const toIndex = _.find(g, i => i.node == node);

					toIndex.node = fromIndex.node;
					fromIndex.node = fromIndex.originalNode;
				}
			})
			.value();

		const moveToDo = _.chain(indexes)
			.filter(i=>i.node != i.originalNode)
			.sortBy('store').reverse()
			.value();
		
		options.debug && stats(indexes, moveToDo);
		options.showCommand && showAllMoves(moveToDo);
		options.commit && await applyAllMoves(moveToDo);
	}

	// restore any ES rebalancing
	await allowRebalancing(true);
})()
