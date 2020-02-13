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

  public void attachSession(Map<String, String> hints, Long timeout) throws OdpsException {
    log.info("attachSession:" + sessionName);
    try {
      session = Session.attach(odps, sessionName, hints, timeout, OdpsStatement.getDefaultTaskName());
      if (session == null || !session.isStarted()) {
        throw new OdpsException("Attach session failed:" + session.getLogView());
      }
      if (!StringUtils.isNullOrEmpty(session.getStartSessionMessage())) {
        log.info(session.getStartSessionMessage());
        log.info("attachSession success:" + sessionName + ",id:" + session.getInstance().getId());
      }
    } catch (OdpsException e) {
      log.error("attachSession failed:" + e.toString());
      throw e;
    }
  }

  public void detachSession() throws OdpsException {
    if (attached()) {
      try {
        log.info("detachSession:" + sessionName + ", id:" + session.getInstance().getId());
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
