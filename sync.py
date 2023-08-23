import pandas as pd
from datetime import datetime, timezone
import time
import pytz
import logging
import requests
import base64
import json
from threading import *
import multiprocessing
import os
import chardet
import re
import argparse
import hmac
import hashlib
from variantwrapper import Variantapi
from fishwrapper import Fishbowlapi

class varianceSync():

	def __init__(self, args):

		self.vWrapper = Variantapi(logging)
		# self.fbWrapper = Fishbowlapi('admin', 'Admin@123', 'localhost')
		
		csvName = args['product_csv']
		with open(csvName, 'rb') as f:
			result = chardet.detect(f.read())
		encoding = result['encoding']

		self.productCSV = pd.read_csv(csvName, encoding=encoding, low_memory=False)

	def startSync(self):
		self.preprocess_attri()

		uniqueParts = self.productCSV['PartNumber'].dropna().unique()
		for part in uniqueParts:
			variableCSV = self.productCSV[self.productCSV['PartNumber'] == part].copy()

			variableProductID = self.createVariableProduct(part, variableCSV)
			self.updateDB(variableProductID, variableCSV)
			self.call_update_api_woo(variableProductID)

	def preprocess_attri(self):
		result = [i for i in self.productCSV if i.startswith('CF-Attri')]
		attri_csv = self.productCSV[result].copy()

		for columnName in attri_csv:
			uniqueTerms = attri_csv[columnName].dropna().unique()

			print(columnName, uniqueTerms)
			attri_name = columnName.replace('CF-Attri ', '')

			self.check_attributes(attri_name, uniqueTerms)


	def createVariableProduct(self, partNumber, csvData):
		# pSKU, pName, pDetail = self.fbWrapper.get_part(partNumber)
	
		pSKU = "WC-0000002"
		pName = "ks-test"
		pDetail = "test"
		result = [i for i in csvData if i.startswith('CF-Attri')]
		attri_csv = csvData[result].copy()

		arr_attri = []
		for columnName in attri_csv:
			uniqueTerms = attri_csv[columnName].dropna().unique()

			if len(uniqueTerms) == 0:
				continue

			attri_name = columnName.replace('CF-Attri ', '')
			attri_id = self.vWrapper.get_woo_attri_id(attri_name)

			arr_attri.append({'id': attri_id, 'options': uniqueTerms.tolist()})

		"""
		name: FB Part name
		description: FB Part description
		sku: FB Part number
		"""
		variable_product_data = {
			"name": pName,
			"description": pDetail,
			"sku": partNumber,
			"attributes": arr_attri
		}
		
		variable_product_id = self.vWrapper.create_variable_product(variable_product_data)
		logging.info(f'Variable Product ID: {variable_product_id} created successfully.')
		return variable_product_id

	def updateDB(self, variableProductID, csvData):

		
		arr_woo_single_products = self.get_synced_single_woo_products(csvData)
		logging.info(arr_woo_single_products)

		arr_fb_single_products = []
		for single_product in arr_woo_single_products:
			arr_fb_single_products.append(self.get_product_by_woo_product_hook(single_product['id'], single_product['sku'], csvData))
		logging.info(arr_fb_single_products)
		
		arr_data = []

		for single_fb_product in arr_fb_single_products:
			search_key = "CF-Attri"
			arrAttriKeys = [key for key, val in single_fb_product.items() if search_key in key]

			woo_attri_info = []
			for attri_key in arrAttriKeys:
				taxonomy, term_id, term_taxonomy_id, slug = self.vWrapper.get_attri_info(attri_key.replace(f"{search_key} ", ""), single_fb_product[attri_key])

				attri = {
					'taxonomy': taxonomy,
					'term': single_fb_product[attri_key],
					'term_id': term_id,
					'slug': slug,
					'term_taxonomy_id': term_taxonomy_id,
					'attri_label': attri_key.replace(f"{search_key} ", "")
				}

				woo_attri_info.append(attri)

			obj = single_fb_product
			obj['attri'] = woo_attri_info

			arr_data.append(obj)

		logging.info(arr_data)

		self.vWrapper.update_table_force(variableProductID, arr_data)


	def get_synced_single_woo_products(self, csvData):

		arrSKU = [ row['ProductNumber'] for index, row in csvData.iterrows()]
		print('skus', arrSKU)
		arr_woo_single_products = self.vWrapper.search_woo_product_by_skus(arrSKU)

		# arr_woo_single_products = [
		# 	{
		# 		"id":50, #Blue
		# 		"name":"Tire and Wheel Kit",
		# 		"sku":"AA-P-01"
		# 	},
		# 	{
		# 		"id":51, #Red
		# 		"name":"Tire and Wheel Kit",
		# 		"sku":"AA-P-02"
		# 	},
		# ]

		return arr_woo_single_products

	def get_product_by_woo_product_hook(self, woo_single_product_id, woo_single_product_sku, csvData):

		filterCSV = csvData.loc[csvData['ProductNumber'] == woo_single_product_sku].copy()
		result = [i for i in csvData if i.startswith('CF-Attri')]

		for index, row in filterCSV.iterrows():
			variance = row
			
			obj = {
				"ProductNum": woo_single_product_sku,
				"PartNum": variance['PartNumber'],
				"WooID": woo_single_product_id
			}

			for attri_key in result:
				if not pd.isnull(variance[attri_key]):
					obj[attri_key] = variance[attri_key]
			return obj

		# self.fbWrapper.get_product(woo_single_product_sku)
		# if woo_single_product_sku == 'AA-P-01':
			
		# 	return {
		# 		"ProductNum": woo_single_product_sku,
		# 		"PartNum": "WC-0000001",
		# 		"CF-Attri Color": "Blue",
		# 		"WooID": woo_single_product_id
		# 	}
		# else:
		# 	return {
		# 		"ProductNum": woo_single_product_sku,
		# 		"PartNum": "WC-0000001",
		# 		"CF-Attri Color": "Red",
		# 		"WooID": woo_single_product_id
		# 	}

	def call_update_api_woo(self, variableProductID):
		self.vWrapper.call_update_variable_woo(variableProductID)

	def search_by_sku(self, sku):
		return self.vWrapper.search_woo_product_by_sku(sku)

	def check_attributes(self, attri_name, attri_terms):
		self.vWrapper.search_woo_attri(attri_name, attri_terms)



if __name__ == "__main__":
	logging.basicConfig(format='%(asctime)s %(message)s', filename='log.log', level=logging.INFO)

	parser = argparse.ArgumentParser()
	parser.add_argument('-f', '--product_csv', required=True, help='Product Tree csv')

	args = vars(parser.parse_args())
	
	sync = varianceSync(args)
	sync.startSync()
