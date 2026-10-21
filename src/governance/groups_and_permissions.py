# Databricks notebook source
# MAGIC %sql
# MAGIC -- Engineering: full build/operate access, both environments
# MAGIC GRANT ALL PRIVILEGES ON CATALOG maven_test TO `data_engineers`;
# MAGIC GRANT ALL PRIVILEGES ON CATALOG maven_dev TO `data_engineers`;
# MAGIC
# MAGIC -- Analyst: read-only, Gold only, dev environment only
# MAGIC GRANT USE CATALOG ON CATALOG maven_dev TO `data_analysts`;
# MAGIC GRANT USE SCHEMA ON SCHEMA maven_dev.gold TO `data_analysts`;
# MAGIC GRANT SELECT ON SCHEMA maven_dev.gold TO `data_analysts`;
# MAGIC
# MAGIC -- Manager: corrected to `regional_managers` matching UI
# MAGIC GRANT USE CATALOG ON CATALOG maven_dev TO `regional_managers`;
# MAGIC GRANT USE SCHEMA ON SCHEMA maven_dev.gold TO `regional_managers`;
# MAGIC GRANT SELECT ON SCHEMA maven_dev.gold TO `regional_managers`;
# MAGIC
# MAGIC -- Executive: corrected to `executives` matching UI
# MAGIC GRANT USE CATALOG ON CATALOG maven_dev TO `executives`;
# MAGIC GRANT USE SCHEMA ON SCHEMA maven_dev.gold TO `executives`;
# MAGIC GRANT SELECT ON SCHEMA maven_dev.gold TO `executives`;
# MAGIC
# MAGIC -- Administrative: covered by existing account/workspace admin rights, no separate grant needed
