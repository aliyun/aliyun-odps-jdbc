package com.aliyun.odps.jdbc;

import java.sql.Wrapper;

public class WrapperAdapter implements Wrapper {

  public WrapperAdapter() {
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface != null && iface.isInstance(this);

  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) {
    if (iface == null) {
      return null;
    }

    if (iface.isInstance(this)) {
      return (T) this;
    }

    return null;
  }

}
