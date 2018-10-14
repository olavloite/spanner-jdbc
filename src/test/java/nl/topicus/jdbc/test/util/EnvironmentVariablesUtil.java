package nl.topicus.jdbc.test.util;

import java.lang.reflect.Field;
import java.util.Map;
import com.google.auth.oauth2.GoogleCredentials;

/**
 * Injecting the credentials environment variable is based on the blog post of Sebastian Daschner:
 * https://blog.sebastian-daschner.com/entries/changing_env_java.
 */
public class EnvironmentVariablesUtil {

  public static void clearCachedDefaultCredentials() throws Exception {
    Field providerField = GoogleCredentials.class.getDeclaredField("defaultCredentialsProvider");
    providerField.setAccessible(true);
    Object provider = providerField.get(null);
    Field cachedField = provider.getClass().getDeclaredField("cachedCredentials");
    cachedField.setAccessible(true);
    cachedField.set(provider, null);
  }

  @SuppressWarnings("unchecked")
  public static void injectEnvironmentVariable(String key, String value) throws Exception {
    Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment");

    Field unmodifiableMapField =
        getAccessibleField(processEnvironment, "theUnmodifiableEnvironment");
    Object unmodifiableMap = unmodifiableMapField.get(null);
    injectIntoUnmodifiableMap(key, value, unmodifiableMap);

    Field mapField = getAccessibleField(processEnvironment, "theEnvironment");
    Map<String, String> map = (Map<String, String>) mapField.get(null);
    map.put(key, value);
  }

  private static Field getAccessibleField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {

    Field field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field;
  }

  @SuppressWarnings("unchecked")
  private static void injectIntoUnmodifiableMap(String key, String value, Object map)
      throws ReflectiveOperationException {

    @SuppressWarnings("rawtypes")
    Class unmodifiableMap = Class.forName("java.util.Collections$UnmodifiableMap");
    Field field = getAccessibleField(unmodifiableMap, "m");
    Object obj = field.get(map);
    ((Map<String, String>) obj).put(key, value);
  }

}
