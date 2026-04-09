import asyncio
from sui_graphql_client import GraphQLClient

async def main():
    # SUI GraphQL endpoint (示例)
    client = GraphQLClient(
        base_url="https://graphql.mainnet.sui.io/graphql",
        token=None  # 如果有 token，填这里
    )

    query = """
    query {
      transaction(digest: "HujCAmwbWXGkjAtup6nQEvkrHTkMD3zW711sZEjGXbh7") {
        gasInput {
          gasSponsor {
            address
          }
          gasPrice
          gasBudget
        }
        effects {
          status
          timestamp
          checkpoint {
            sequenceNumber
          }
          epoch {
            epochId
            referenceGasPrice
          }
        }
      }
    }
    """

    result = await client.execute(query)
    print("GraphQL result:", result)

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())