package com.aliyun.odps.jdbc;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Session;
import com.aliyun.odps.jdbc.utils.OdpsLogger;
import com.aliyun.odps.utils.StringUtils;

import java.util.Map;

/**
 * Created by dongxiao on 2019/10/15.
 */
public class OdpsSessionManager {

  private String sessionName;
  private Odps odps = null;
  private Session session = null;
  private OdpsLogger log = null;

  OdpsSessionManager(String sessionName, Odps odps, OdpsLogger log) {
    this.odps = odps;
    this.sessionName = sessionName;
    this.log = log;
  }

  public Session getSessionInstance() {
    return session;
  }

  public String getSessionId() {
    if (attached()) {
      return session.getInstance().getId();
    }
    return null;
  }

  public void attachSession(Map<String, String> hints, Long timeout) throws OdpsException {
    log.debug("attachSession:" + sessionName);
    try {
      session = Session.attach(odps, sessionName, hints, timeout, OdpsStatement.getDefaultTaskName());
      if (session == null || !session.isStarted()) {
        throw new OdpsException("Attach session failed:" + session.getLogView());
      }
      if (!StringUtils.isNullOrEmpty(session.getStartSessionMessage())) {
        log.info(session.getStartSessionMessage());
      }
    } catch (OdpsException e) {
      log.error("attachSession failed:" + e.toString());
      throw e;
    }
  }

  public void detachSession() throws OdpsException {
    log.debug("detachSession:" + sessionName);
    if (attached()) {
      try {
        session.stop();
      } catch (OdpsException e) {
        log.error("detachSession failed:" + e.toString());
        throw e;
      }
    }
  }

  public boolean attached() {
    return session != null;
  }
}
