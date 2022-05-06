package com.aliyun.odps.jdbc.utils;

import com.aliyun.odps.Odps;

/**
 * get/set catalog/schema depends on odpsNamespaceSchema flag
 */
public class CatalogSchema {

  private Odps odps;
  private boolean twoTier = true;

  public CatalogSchema(Odps odps, boolean odpsNamespaceSchema) {
    this.odps = odps;
    this.twoTier = !odpsNamespaceSchema;
  }

  public CatalogSchema(String catalog, String schema, String tableName) {

  }

  public String getCatalog() {
    if (twoTier) {
      return null;
    } else {
      return odps.getDefaultProject();
    }
  }

  public String getSchema() {
    if (twoTier) {
      return odps.getDefaultProject();
    } else {
      return odps.getCurrentSchema();
    }
  }

  public void setCatalog(String catalog) {
    if (!twoTier) {
      odps.setDefaultProject(catalog);
    }
  }

  public void setSchema(String schema) {
    if (twoTier) {
      this.odps.setDefaultProject(schema);
    } else {
      this.odps.setCurrentSchema(schema);
    }
  }

}

