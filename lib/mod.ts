type Promisable<T> = T | Promise<T>;

/** identifies a resource */
export type ResourceId = string | number;

type BatchedFetchMap<Id extends ResourceId, Resource, Query = Id> = {
	/** grouping key */
	key: string | number | undefined;
	/** timer for batch execution */
	timeout: ReturnType<typeof setTimeout> | undefined;
	/** controls the lifecycle of this batch */
	controller: AbortController;
	/** a map of pending queries */
	pending: Map<
		Id,
		{
			/** original query descriptor */
			query: Query;
			/** promise that will resolve with the resource */
			deferred: PromiseWithResolvers<Resource>;
			/** whether we have a consumer without an abort signal */
			passive: boolean;
			/** amount of consumers with an abort signal */
			signals: number;
		}
	>;
};

type BaseOptions<Id extends ResourceId, Resource, Query = Id> = {
	/**
	 * maximum number of queries that can be included in one request
	 */
	limit: number;

	/**
	 * how long to wait for new queries to be collected before a request
	 * @default 125
	 */
	timeout?: number;

	/**
	 * performs the request
	 * @param queries queries passed for this batch
	 * @param signal abort signal
	 * @returns array of resources
	 */
	fetch: (queries: Query[], signal: AbortSignal) => Promisable<Resource[]>;

	/**
	 * optional function for separating queries into different batch
	 * @param query query descriptor
	 * @returns batch grouping key
	 */
	key?: (query: Query) => string | number;

	/**
	 * function that takes in the resource's identifier, used to associate
	 * resources with the queries
	 * @param resource resource
	 * @returns resource identifier
	 */
	idFromResource: (resource: Resource) => Id;
};

/** options for batch fetching */
export type BatchedFetchOptions<Id extends ResourceId, Resource, Query = Id> =
	& BaseOptions<Id, Resource, Query>
	& (Query extends Id ? {
			/**
			 * function that takes in the resource identifier from the query descriptor,
			 * used for deduplication and resource matching.
			 * @param query query descriptor
			 * @returns resource identifier
			 */
			idFromQuery?: (query: Query) => Id;
		}
		: {
			/**
			 * function that takes in the resource identifier from the query descriptor,
			 * used for deduplication and resource matching.
			 * @param query query descriptor
			 * @returns resource identifier
			 */
			idFromQuery: (query: Query) => Id;
		});

/** error thrown when a resource wasn't returned in the response */
export class ResourceMissingError extends Error {
	override readonly name = 'ResourceMissingError';
}

const identity = <T>(value: T): T => value;

/**
 * creates a function that batches individual queries into one single request.
 * @param options configurations
 * @returns a function that you can use to request for a query.
 */
/*#__NO_SIDE_EFFECTS__*/
export const createBatchedFetch = <Id extends ResourceId, Resource, Query = Id>(
	options: BatchedFetchOptions<Id, Resource, Query>,
): (query: Query, signal?: AbortSignal) => Promise<Resource> => {
	const {
		limit,
		timeout = 125,
		fetch,
		key: _key,
		idFromQuery = identity,
		idFromResource,
	} = options;

	/** current active batch */
	let curr: BatchedFetchMap<Id, Resource, Query> | undefined;

	return (query: Query, signal?: AbortSignal): Promise<Resource> => {
		// throw early if provided signal is already aborted
		signal?.throwIfAborted();

		const id = idFromQuery(query);
		const key = _key?.(query);

		// create a new batch if:
		// - we don't have a batch currently waiting
		// - the current batch has already reached the limit
		// - batch key doesn't match
		let batch = curr;
		if (batch === undefined || batch.pending.size >= limit || batch.key !== key) {
			batch = curr = {
				key,
				timeout: undefined,
				controller: new AbortController(),
				pending: new Map(),
			};
		}

		let meta = batch.pending.get(id);
		if (meta === undefined) {
			meta = {
				query: query,
				deferred: Promise.withResolvers(),
				passive: false,
				signals: 0,
			};

			batch.pending.set(id, meta);
		}

		let promise = meta.deferred.promise;

		if (signal === undefined) {
			// this consumer provided no signal, so we can't consider this query for
			// removal if a different consumer has aborted theirs.
			meta.passive = true;
		} else {
			// we need the returned promise to resolve early if the signal is aborted.
			// so we'll race it with this deferred that will only throw.
			const def = Promise.withResolvers<never>();
			promise = Promise.race([promise, def.promise]);

			// make this signal count
			meta.signals++;

			signal.addEventListener(
				'abort',
				() => {
					// immediately reject this consumer's promise
					def.reject(signal.reason);

					// decrement the count
					meta.signals--;

					// return early, have the query remain in batch if:
					// - we have passive consumers waiting on this query
					// - there are still other consumers with an abort signal waiting
					if (meta.passive || meta.signals > 0) {
						return;
					}

					// no more consumers care about this query, remove from batch
					batch.pending.delete(id);

					// return early, have the batch continue execution if we still need
					// to process other queries.
					if (batch.pending.size > 0) {
						return;
					}

					// batch is empty, clean up completely
					batch.controller.abort();
					clearTimeout(batch.timeout);

					if (curr === batch) {
						curr = undefined;
					}
				},
				{
					once: true,
					signal: batch.controller.signal,
				},
			);
		}

		{
			// reset the execution timer
			clearTimeout(batch.timeout);

			batch.timeout = setTimeout(() => {
				if (curr === batch) {
					curr = undefined;
				}

				perform(batch, fetch, idFromResource);
			}, timeout);
		}

		return promise;
	};
};

const perform = async <Id extends ResourceId, Resource, Query = Id>(
	map: BatchedFetchMap<Id, Resource, Query>,
	fetch: (queries: Query[], signal: AbortSignal) => Promisable<Resource[]>,
	idFromResource: (data: Resource) => Id,
) => {
	const signal = map.controller.signal;
	if (signal.aborted) {
		return;
	}

	const pending = map.pending;
	if (pending.size === 0) {
		// theoretically this should only be empty if the whole-batch signal is
		// aborted, but better be safe.
		return;
	}

	let errored = false;

	try {
		const queries = Array.from(pending.values(), (meta) => meta.query);
		const dataset = await fetch(queries, signal);

		for (const data of dataset) {
			const id = idFromResource(data);
			const meta = pending.get(id);

			meta?.deferred.resolve(data);
		}
	} catch (error) {
		errored = true;

		for (const meta of pending.values()) {
			meta.deferred.reject(error);
		}
	} finally {
		if (!errored) {
			// we've succeeded! we're iterating the pending map again to boot
			// unresolved promises, else they'll end up waiting forever.
			//
			// this should only apply for scenarios where the caller/API handles
			// nonexistent data by omitting it entirely from the results.
			for (const meta of pending.values()) {
				meta.deferred.reject(new ResourceMissingError());
			}
		}
	}

	// abort the controller to clean up event listeners to upstream signals
	map.controller.abort();
};
