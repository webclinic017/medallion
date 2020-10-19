#%%
from requests import request
import os
base_url = 'https://simfin.com/api/v2/'
api_key = 'N37JIMgUfeCf6QUIuzlzCZVXbgA9Ivc8'

#%%
def getListOfAllCompanies():
  url = base_url + 'companies/list'
  params = {'api-key': api_key}
  return request('GET', url, params=params).json()

#%%
def getCompanyPrices(ticker):
  url = base_url + 'companies/prices'
  params = {'api-key': api_key, 'ticker': ticker}
  return request('GET', url, params=params)

#%%
def getCompaniesPrices(companies):
  for company in companies:
    prices = getCompanyPrices('FB')""" company[1] """
    break
  return prices

#%%
companies = getListOfAllCompanies()

#%%
prices = getCompaniesPrices(companies['data'])

#%%
priceData = prices.json()[0]['data']
print(priceData[0])
print(priceData[-1])
print(len(priceData))
# %%
