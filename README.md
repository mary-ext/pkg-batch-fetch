# batch-fetch

[JSR](https://jsr.io/@mary/batch-fetch) | [source code](https://tangled.sh/@mary.my.id/pkg-batch-fetch)

utility for batching individual queries into one single request.

```ts
type UserId = ReturnType<typeof crypto.randomUUID>;
interface User {
	id: UserId;
	name: string;
}

const fetchUser = createBatchedFetch<UserId, User>({
	limit: 50,
	async fetch(userIds, signal) {
		const url = new URL(`/api/users`, location.origin);
		for (const id of userIds) {
			url.searchParams.append('ids', id);
		}

		const response = await fetch(url, { signal });

		return await response.json() as unknown as User[];
	},
	idFromResource: (user) => user.id,
});

// these individual queries will be batched into one
{
	const p1 = fetchUser('c83e7271-c865-4eae-8c6a-d6f21acb17c1');
	const p2 = fetchUser('306e38a2-bea6-4bf7-80ea-21822aa24c40');

	const [user1, user2] = await Promise.all([p1, p2]);
	// ...
}
```
