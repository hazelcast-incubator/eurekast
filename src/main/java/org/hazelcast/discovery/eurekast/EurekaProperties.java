package org.hazelcast.discovery.eurekast;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.config.properties.ValueValidator;
import com.hazelcast.core.TypeConverter;

import java.io.File;

import static com.hazelcast.config.properties.PropertyTypeConverter.INTEGER;
import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;

public final class EurekaProperties {

    private static final TypeConverter FILE_TYPE_CONVERTER = new TypeConverter() {

        @Override
        public Comparable convert(Comparable value) {
            if (value instanceof String) {
                File file = new File((String) value);
                if (!file.exists()) {
                    throw new IllegalArgumentException("Defined file '" + value + "' does not exist");
                }
                if (!file.isDirectory()) {
                    throw new IllegalArgumentException("Defined file '" + value + "' is a directory");
                }
                if (!file.canRead()) {
                    throw new IllegalArgumentException("Defined file '" + value + "' is not readable");
                }
                return file;

            } else {
                throw new IllegalArgumentException("Cannot convert to java.io.File");
            }
        }
    };

    public static final PropertyDefinition APPLICATION_NAME = property("application-name", STRING);

    public static final PropertyDefinition RESOLVER_HOSTNAME = optional("resolver.hostname", STRING);
    public static final PropertyDefinition RESOLVER_DNS = optional("resolver.dns", STRING);
    public static final PropertyDefinition RESOLVER_FILE = optional("resolver.file", FILE_TYPE_CONVERTER);

    public static final PropertyDefinition RESOLVER_WRITER_PORT = property("resolver.writer.port", INTEGER);
    public static final PropertyDefinition RESOLVER_READER_PORT = property("resolver.reader.port", INTEGER);

    // Prevent instantiation
    private EurekaProperties() {
    }

    private static PropertyDefinition property(String key, TypeConverter typeConverter) {
        return property(key, typeConverter, null);
    }

    private static PropertyDefinition optional(String key, TypeConverter typeConverter) {
        return optional(key, typeConverter, null);
    }

    private static PropertyDefinition property(String key, TypeConverter typeConverter, ValueValidator valueValidator) {
        return new SimplePropertyDefinition(key, false, typeConverter, valueValidator);
    }

    private static PropertyDefinition optional(String key, TypeConverter typeConverter, ValueValidator valueValidator) {
        return new SimplePropertyDefinition(key, true, typeConverter, valueValidator);
    }
}
