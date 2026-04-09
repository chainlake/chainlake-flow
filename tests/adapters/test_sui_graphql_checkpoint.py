import asyncio
from sui_graphql_client import GraphQLClient

async def main():
    # SUI GraphQL endpoint (示例)
    client = GraphQLClient(
        base_url="https://graphql.mainnet.sui.io/graphql",
        token=None  # 如果有 token，填这里
    )

    query = """
    checkpoints(
      first: Int
      after: String
      last: Int
      before: String
      filter: CheckpointFilter
    ): CheckpointConnection
    """

    result = await client.execute(query)
    edges = result.get("data", {}).get("checkpoints", {}).get("edges", [])
    if edges:
        latest = edges[0]["node"]
        print("Latest checkpoint:", latest)
    else:
        print("No checkpoints found")

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())