package com.continuuity.common.logging;

/**
 * A logging context when the type of entity is not known. This logging context has limited functionality.
 */
public class GenericLoggingContext extends ApplicationLoggingContext {
  public static final String TAG_ENTITY_ID = ".entityId";

  /**
   * Constructs the GenericLoggingContext.
   * @param accountId account id
   * @param applicationId application id
   * @param entityId flow entity id
   */
  public GenericLoggingContext(final String accountId,
                                 final String applicationId,
                                 final String entityId) {
    super(accountId, applicationId);
    setSystemTag(TAG_ENTITY_ID, entityId);
  }

  @Override
  public String getLogPartition() {
    throw new UnsupportedOperationException("GenericLoggingContext does not support this");
  }

  @Override
  public String getLogPathFragment() {
    throw new UnsupportedOperationException("GenericLoggingContext does not support this");
  }
}
