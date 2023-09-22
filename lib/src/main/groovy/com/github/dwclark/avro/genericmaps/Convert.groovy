package com.github.dwclark.avro.genericmaps

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecordBuilder
import java.util.function.BiFunction
import groovy.transform.CompileStatic

@CompileStatic
class Convert {

    static abstract class Base {
	final Map<Schema.Type, BiFunction<Schema,Object,Object>> baseConverters = 
	    [(Schema.Type.ARRAY): this::convertArray as BiFunction<Schema,Object,Object>,
	     (Schema.Type.BOOLEAN): this::identity as BiFunction<Schema,Object,Object>,
	     (Schema.Type.BYTES): this::identity as BiFunction<Schema,Object,Object>,
	     (Schema.Type.DOUBLE): this::identity as BiFunction<Schema,Object,Object>,
	     (Schema.Type.ENUM): this::identity as BiFunction<Schema,Object,Object>,
	     (Schema.Type.FIXED): this::identity as BiFunction<Schema,Object,Object>,
	     (Schema.Type.FLOAT): this::identity as BiFunction<Schema,Object,Object>,
	     (Schema.Type.INT): this::identity as BiFunction<Schema,Object,Object>,
	     (Schema.Type.LONG): this::identity as BiFunction<Schema,Object,Object>,
	     (Schema.Type.MAP): this::convertMap as BiFunction<Schema,Object,Object>,
	     (Schema.Type.RECORD): this::exception as BiFunction<Schema,Object,Object>,
	     (Schema.Type.STRING): this::identity as BiFunction<Schema,Object,Object>,
	     (Schema.Type.UNION): this::exception as BiFunction<Schema,Object,Object>].asImmutable()

	abstract BiFunction<Schema,Object,Object> converter(final Schema.Type type);
	
	Object exception(final Schema schema, final Object value) {
	    throw new RuntimeException("Can't handle schema with value ${value}")
	}
	
	Object identity(final Schema schema, final Object value) {
	    return value
	}
	
	List convertArray(final Schema schema, final Object value) {
	    final List list = (value instanceof List) ? (List) value : List.copyOf((Collection) value)
	    final Schema elementSchema = schema.elementType
	    return list.collect { o -> converter(elementSchema.type).apply(elementSchema, o) }
	}

	Map<String,Object> convertMap(final Schema schema, final Object value) {
	    final Schema valueSchema = schema.valueType
	    final Map<String,Object> map = (Map<String,Object>) value
	    return map.inject([:]) { Map<String,Object> ret, String k, Object v ->
		ret << new MapEntry(k, converter(valueSchema.type).apply(valueSchema, v))
	    }
	}
    }

    static class ToMap extends Base {
	final Map<Schema.Type, BiFunction<Schema,Object,Object>> converters =
	    (baseConverters + [(Schema.Type.RECORD): this::convert as BiFunction<Schema,Object,Object>]).asImmutable()
	
	Map<String,Object> convert(final Schema schema, final Object rec) {
	    final Map<String,Object> ret = [:]
	    final Record record = (Record) rec
	    
	    record.schema.fields.each { Schema.Field field ->
		final Schema fieldSchema = field.schema()
		final Object value = record.get(field.name())
		ret[field.name()] = (value == null) ? null : converter(fieldSchema.type).apply(fieldSchema, value)
	    }
	    
	    return ret
	}

	BiFunction<Schema,Object,Object> converter(final Schema.Type type) {
	    converters[type]
	}
    }

    static class ToRecord extends Base {
	final Map<Schema.Type, BiFunction<Schema,Object,Object>> converters =
	    (baseConverters + [(Schema.Type.RECORD): this::convert as BiFunction<Schema,Object,Object>,
			       (Schema.Type.UNION): this::union as BiFunction<Schema,Object,Object>]).asImmutable()
		
	Record convert(final Schema schema, final Object obj) {
	    final Map<String,Object> map = (Map<String,Object>) obj
	    final GenericRecordBuilder builder = new GenericRecordBuilder(schema)
	    
	    schema.fields.each { Schema.Field field ->
		final Schema fieldSchema = field.schema()
		if(map.containsKey(field.name())) {
		    final Object value = map[field.name()]
		    if(value == null) {
			builder.set(field.name(), null)
		    }
		    else {
			builder.set(field.name(), converter(fieldSchema.type).apply(fieldSchema, value))
		    }
		}
	    }
	
	    return builder.build()
	}

	Object union(final Schema schema, final Object value) {
	    final Schema last = schema.types.last()
	    return converter(last.type).apply(last, value)
	}

	BiFunction<Schema,Object,Object> converter(final Schema.Type type) {
	    converters[type]
	}
    }

    private static final ToMap _toMap = new ToMap();
    private static final ToRecord _toRecord = new ToRecord();
    
    static Map<String,Object> toMap(Record record) {
	_toMap.convert(record.schema, record)
    }

    static Record toRecord(Schema schema, Map<String,Object> map) {
	_toRecord.convert(schema, map)
    }
}
