print("hehe")

try:
# Databricks notebook source
    dbutils.widgets.text("catalog", "maven_test")
    catalog = dbutils.widgets.get("catalog")

    print(f"Running pipeline for catalog: {catalog}")

except:
    print("shit")