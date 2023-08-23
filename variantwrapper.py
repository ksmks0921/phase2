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
from woocommerce import API
import argparse
import hmac
import hashlib
import mysql.connector

class Variantapi():
	
	def __init__(self, logers):
		self.vLog = logers
		
		woo_consumer_key = "ck_b9bf66a415c052cf7d2c5aee0aa22327ae17a74e"
		woo_consumer_secret = "cs_8d039f67d307448b46d8e4e88c0bd351a7e7e183"
		self.host = "https://woocommerce-1003342-3536661.cloudwaysapps.com/"
		

		self.wcapi = API(
			url=self.host,
			consumer_key=woo_consumer_key,
			consumer_secret=woo_consumer_secret,
			timeout=600,
			version="wc/v3",
			wp_api=True,
		)

		self.sqldb = mysql.connector.connect(
			host="45.76.16.241",
			user="ktcukrnkjh",
			password="vsE5Uj4tE6",
			database="ktcukrnkjh"
		)
		# self.sqldb = mysql.connector.connect(
		# 	host="localhost",
		# 	user="root",
		# 	password="",
		# 	database="ktcukrnkjh"
		# )
	def create_variable_product(self, productData):
		self.vLog.info('Create variable Product')
		data = {
			"name": productData['name'],
			"type": "variable",
			"stock_status": "instock",
			"description": productData['description'],
			"short_description": productData['description'],
			"sku": productData['sku'],
			"purchasable":True			
		}

		arr_attri = []
		sequence = 0
		for attri in productData['attributes']:
			obj = {
				"id": attri['id'],
				"position": sequence,
				"options": attri['options'],
				"variation": True
			}

			arr_attri.append(obj)
			sequence += 1
		data['attributes'] = arr_attri

		self.vLog.info(data)
		res = self.wcapi.post("products", data).json()	
		self.vLog.info("res")
		self.vLog.info(res)

		return res['id']

	def call_update_variable_woo(self, product_id):
		self.vLog.info(self.wcapi.put(f"products/{product_id}", {}).json())
		self.vLog.info(f"Variable Product ID: {product_id} updated via API")

	def get_attri_info(self, attri_key, attri_value):

		self.vLog.info('get attribute info')
		mycursor = self.sqldb.cursor(buffered=True , dictionary=True)

		mycursor.execute(f"SELECT * FROM wp_woocommerce_attribute_taxonomies WHERE attribute_label = '{attri_key}' AND attribute_type = 'select'")
		attribute_taxonomy = mycursor.fetchone()

		taxonomy = f"pa_{attribute_taxonomy['attribute_name']}"

		mycursor.execute(f"SELECT * FROM wp_terms WHERE name = '{attri_value}'")
		attribute_term = mycursor.fetchone()

		mycursor.execute(f"SELECT * FROM wp_term_taxonomy WHERE term_id = '{attribute_term['term_id']}' AND taxonomy = '{taxonomy}'")
		attribute_term_taxonomy = mycursor.fetchone()

		mycursor.close()
		self.sqldb.commit()

		self.vLog.info(f"taxonomy = {taxonomy}, term_id = {attribute_term['term_id']}, slug = {attribute_term['slug']}, term_taxonomy_id = {attribute_term_taxonomy['term_taxonomy_id']}")
		return taxonomy, attribute_term['term_id'], attribute_term_taxonomy['term_taxonomy_id'], attribute_term['slug']

	def update_table_force(self, variableProductID, varainces):

		try:
			
			self.sqldb.autocommit = True
			self.sqldb.start_transaction()

			cursor = self.sqldb.cursor()
			
			sequence  = 0
			for variance in varainces:
				sequence += 1

				# # wp_posts
				# str_excerpt = ', '.join([f"{attri['attri_label']}: {attri['term']}" for attri in variance['attri']])
				# query = f"UPDATE wp_posts SET `post_excerpt` = '{str_excerpt}', `comment_status` = 'closed', `post_parent` = '{variableProductID}', `menu_order` = '{sequence}', `post_type` = 'product_variation', `guid` = '{self.host}/?post_type=product_variation&p={variance['WooID']}' WHERE `ID` = '{variance['WooID']}'"
				# self.vLog.info(query)

				# cursor.execute(query)

				# for attri in variance['attri']:
				# 	# wp_wc_product_attributes_lookup
				# 	query = f"INSERT INTO wp_wc_product_attributes_lookup (`product_id`, `product_or_parent_id`, `taxonomy`, `term_id`, `is_variation_attribute`, `in_stock`) VALUES ('{variance['WooID']}', '{variableProductID}', '{attri['taxonomy']}', '{attri['term_id']}', '1', '1')"
				# 	self.vLog.info(query)
				# 	cursor.execute(query)

				# 	# wp_postmeta
				# 	query = f"INSERT INTO wp_postmeta (`post_id`, `meta_key`, `meta_value`) VALUES ('{variance['WooID']}', 'attribute_{attri['taxonomy']}', '{attri['slug']}')"
				# 	self.vLog.info(query)
				# 	cursor.execute(query)
				# 	# query = f"INSERT INTO wp_postmeta (`post_id`, `meta_key`, `meta_value`) VALUES ('{variance['WooID']}', '_thumbnail_id', '0')"
				# 	# self.vLog.info(query)
				# 	# cursor.execute(query)

				# # wp_term_relationships
				# query = f"DELETE FROM wp_term_relationships WHERE `object_id` = '{variance['WooID']}'"
				# self.vLog.info(query)
				# cursor.execute(query)

				# # wp_postmeta
				# query = f"INSERT INTO wp_postmeta (`post_id`, `meta_key`, `meta_value`) VALUES ('{variance['WooID']}', '_variation_description', '')"
				# self.vLog.info(query)
				# cursor.execute(query)

				# # wp_wc_product_meta_lookup
				# query = f"UPDATE wp_wc_product_meta_lookup SET `tax_class` = 'parent' WHERE `product_id` = '{variance['WooID']}'"
				# self.vLog.info(query)
				# cursor.execute(query)


			# my code
				individual_product = self.wcapi.get(f"products/{variance['WooID']}").json()
				individual_product_image = individual_product.get("images", [])[0]["src"]
				variance["individual_image"] = individual_product_image
				# Fetch the variations of the parent product
			parent_variations = self.wcapi.get(f"products/{variableProductID}/variations").json()
			# Assign individual images to variation images
			for parent_variation in parent_variations:
				for variance in varainces:
					if parent_variation["id"] == variance["id"]:
						variation_image = variance["individual_image"]
						self.wcapi.put(f"products/variations/{parent_variation['id']}", {"image": {"src": variation_image}})
						break
			# end of my code

			# wp_wc_product_meta_lookup
			# query = f"UPDATE wp_wc_product_meta_lookup SET `stock_status` = 'instock' WHERE `product_id` = '{variableProductID}'"
			# self.vLog.info(query)
			# cursor.execute(query)

			# # wp_postmeta
			# query = f"UPDATE wp_postmeta SET `meta_value` = 'instock' WHERE `post_id` = '{variableProductID}' AND `meta_key` = '_stock_status'"
			# self.vLog.info(query)
			# cursor.execute(query)

			
			self.sqldb.commit()

			self.vLog.info(f"Database updated successfully for variable product ID: {variableProductID}")

		except mysql.connector.Error as error:
			print("Failed to update record to database rollback: {}".format(error))
			# reverting changes because of exception
			self.sqldb.rollback()
		finally:
			# closing database connection.
			if self.sqldb.is_connected():
				cursor.close()
				print("connection is closed")

	def search_woo_product_by_sku(self, sku):
		res = self.wcapi.get(f"products?filter[sku]={sku}&filter[type]=simple").json()
		if len(res):
			self.vLog.info(res[0])
			return res[0]

		return None

	def search_woo_product_by_skus(self, skus):
		strSKU =",".join(skus) + ','
		res = self.wcapi.get(f"products?sku={strSKU}").json()
		if len(res):
			self.vLog.info(res)
			return res

		return []

	def get_woo_attri_id(self, attri_name):
		mycursor = self.sqldb.cursor(buffered=True , dictionary=True)

		mycursor.execute(f"SELECT * FROM wp_woocommerce_attribute_taxonomies WHERE attribute_label = '{attri_name}' AND attribute_type = 'select'")
		attribute_taxonomy = mycursor.fetchone()

		self.sqldb.commit()
		mycursor.close()

		self.vLog.info(attribute_taxonomy)

		return attribute_taxonomy['attribute_id']

	def search_woo_attri(self, attri_name, attri_terms):
		mycursor = self.sqldb.cursor(buffered=True , dictionary=True)

		mycursor.execute(f"SELECT * FROM wp_woocommerce_attribute_taxonomies WHERE attribute_label = '{attri_name}' AND attribute_type = 'select'")
		attribute_taxonomy = mycursor.fetchone()

		self.sqldb.commit()
		mycursor.close()

		self.vLog.info(attribute_taxonomy)

		if attribute_taxonomy == None:
			print('Create attribute')

			data = {
				"name": attri_name,
				"slug": attri_name,
				"type": "select",
				"order_by": "menu_order"
			}

			jsonAttri = self.wcapi.post("products/attributes", data).json()
			self.vLog.info(jsonAttri)

			attri_id = jsonAttri['id']

			print('Create Terms')

			terms = [{"name": value} for value in attri_terms]
			batchData = {
				 "create": terms
			}
			jsonterm = self.wcapi.post(f"products/attributes/{attri_id}/terms/batch", batchData).json()
			self.vLog.info(jsonterm)

		else:
			existing_attri_id = attribute_taxonomy['attribute_id']

			jsonterm = self.wcapi.get(f"products/attributes/{existing_attri_id}/terms").json()

			non_existing_terms = []

			for prod_term in attri_terms:
				bExist = False
				for term in jsonterm:
					if prod_term == term['name']:
						bExist = True
						break
				if bExist == False:
					non_existing_terms.append(prod_term)

			print(non_existing_terms)
			if (len(non_existing_terms) > 0):

				print('Add Terms')

				terms = [{"name": value} for value in non_existing_terms]
				batchData = {
					 "create": terms
				}
				jsonterm = self.wcapi.post(f"products/attributes/{existing_attri_id}/terms/batch", batchData).json()
				self.vLog.info(jsonterm)
