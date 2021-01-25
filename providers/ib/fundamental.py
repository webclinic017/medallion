from IbTrader import IbTrader
import sleep

async def main():
	ib = IbTrader()
	await ib.start()
	
	contract = ib.getNasdaqContract('MSFT')
	ib.reqFundamentalData(111, contract=contract, reportType='ReportsFinStatements', fundamentalDataOptions=[])

	time.sleep(5)
	
	ib.stop()

if __name__ == '__main__':
	try:
		asyncio.run(main())
	except:
		print('error')
