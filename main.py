import asyncio
import pandas as pd
from gql import Client, gql
from gql.transport.websockets import WebsocketsTransport
from tabulate import tabulate  # For displaying data as a table
from colorama import Fore, Style


async def run_subscription():
    # Setup WebSocket connection
    transport = WebsocketsTransport(
        url="wss://streaming.bitquery.io/eap?token=ory_at_utHxV4TA-KLdv3NM0h8m_88KHUAuxLMeByYb62vF_iU.Pxe5noJx__EZgsTKxHmmAsG7SC8wNLD5nWzW60fN_J4",
        headers={"Sec-WebSocket-Protocol": "graphql-ws"})

    # Establish the connection
    await transport.connect()
    print("Connected to WebSocket")

    try:
        # Define the expected columns
        expected_columns = [
            'Block.Time', 'Trade.AmountInUSD', 'Trade.Currency.Symbol',
            'Trade.Price', 'Trade.PriceInUSD'
        ]

        tron_price = pd.DataFrame(columns = expected_columns)
        print("RealTime TRON Token Price")
        while True:
            async for result in transport.subscribe(
                    gql("""
                subscription {
  Tron {
    DEXTradeByTokens(
      where: {Trade: {Currency: {SmartContract: {is: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"}}}, TransactionStatus: {Success: true}}
    ) {
      Block {
        Time
      }
      Trade {
        AmountInUSD
		Price
        PriceInUSD
        Currency {
          Symbol
        }
      }
    }
  }
}
                """)):
                if result.data:
                    #  print(result.data)
                    new_data = pd.json_normalize(result.data['Tron']['DEXTradeByTokens'])

                    # Reindex new_data to match expected_columns
                    new_data = new_data.reindex(columns=expected_columns)

                    if tron_price.empty:
                        tron_price = new_data
                    else:
                        tron_price = pd.concat([tron_price, new_data], ignore_index=True)
                    headers = tron_price.columns.tolist()
                    colored_headers = [Fore.BLUE + header + Style.RESET_ALL for header in headers]

                    # Format rows: Price column in green
                    formatted_rows = []
                    for row in tron_price.itertuples(index=False):
                        formatted_row = list(row)
                        formatted_row[3] = Fore.GREEN + str(row[3]) + Style.RESET_ALL
                        formatted_rows.append(formatted_row)

                    # Display DataFrame as a table with color
                    table = tabulate(formatted_rows, headers=colored_headers, tablefmt='pretty', showindex=False)
                    print(Fore.GREEN + table + Style.RESET_ALL)  # Green colored table output
    finally:
        await transport.close()


def main():
    asyncio.run(run_subscription())


if __name__ == "__main__":
    main()