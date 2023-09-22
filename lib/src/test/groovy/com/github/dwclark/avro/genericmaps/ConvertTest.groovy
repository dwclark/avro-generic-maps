package com.github.dwclark.avro.genericmaps

import java.time.*
import org.apache.avro.Schema
import org.apache.avro.generic.*
import spock.lang.Specification
import static Convert.*

class ConvertTest extends Specification {

    Schema loadSchema(final String path) {
	new Schema.Parser().parse(ConvertTest.classLoader.getResourceAsStream(path))
    }
    
    def "test simple record no nulls"() {
        setup:
	def schema = loadSchema("Employee.json")
	def record = new GenericRecordBuilder(schema).tap {
	    set "name", "Scooby"
	    set "age", 42
	    set "weight", 75L
	    set "latitude", 10.0f
	    set "income", 19.0d
	    set "stupid", true
	}.build()
	
        when:
	def map = toMap(record)

        then:
        map.name == "Scooby"
	map.age == 42
	map.weight == 75L
	map.latitude == 10.0f
	map.income == 19.0d
	map.stupid
    }

    def "test simple record no nulls reverse"() {
	setup:
	def schema = loadSchema("Employee.json")
	def map = [ name: "Scooby", age: 42, weight: 75L, latitude: 10.0f, income: 19.0d, stupid: true]
	
        when:
	def record = toRecord(schema, map)

        then:
        record.get("name") == "Scooby"
	record.get("age") == 42
	record.get("weight") == 75L
	record.get("latitude") == 10.0f
	record.get("income") == 19.0d
	record.get("stupid")
    }

    def "test simple record nulls and defaults"() {
        setup:
	def schema = loadSchema("EmployeeDefs.json")
	def record = new GenericRecordBuilder(schema).tap {
	    set "name", "Scooby"
	    set "age", null
	    set "weight", 75L
	    set "latitude", 10.0f
	    set "income", 19.0d
	}.build()
	
        when:
	def map = toMap(record)

        then:
        map.name == "Scooby"
	map.age == null
	map.weight == 75L
	map.latitude == 10.0f
	map.income == 19.0d
	map.stupid
    }

    def "test simple record nulls and defaults reverse"() {
        setup:
	def schema = loadSchema("EmployeeDefs.json")
	def map = [name: "Scooby", age: 21, weight: 75L, latitude: 10.0f, income: 19.0d]
	
        when:
	def record = toRecord(schema, map)

        then:
        record.get("name") == "Scooby"
	record.get("age") == 21
	record.get("weight") == 75L
	record.get("latitude") == 10.0f
	record.get("income") == 19.0d
	record.get("stupid")
    }

    def "test collections with nulls"() {
	setup:
	def schema = loadSchema("Stuff.json")
	def r1 = new GenericRecordBuilder(schema).tap {
	    set "numbers", [1,2,3,4,5]
	    set "namesToAges", [scooby: 10, fred: 25]
	}.build()

	when:
	def m1 = toMap(r1)

	then:
	m1.numbers == [1,2,3,4,5]
	m1.names == null
	m1.namesToAges == [scooby: 10, fred: 25]
    }

    def "test collections with nulls reverse"() {
	setup:
	def schema = loadSchema("Stuff.json")
	def m1 = [numbers: [1,2,3,4,5], namesToAges: [scooby: 10, fred: 25]]
	def m2 = [numbers: [1,2,3,4,5], names: [[first: 'scooby', last: 'doo'], [first: 'happy', last: 'gilmore']],
		  namesToAges: [scooby: 10, fred: 25]]

	when:
	def r1 = toRecord(schema, m1)

	then:
	r1.get("numbers") == [1,2,3,4,5]
	r1.get("names") == null
	r1.get("namesToAges") == [scooby: 10, fred: 25]
	
	when:
	def r2 = toRecord(schema, m2)
	def names = r2.get('names')

	then:
	r2.get("numbers") == [1,2,3,4,5]
	names[0].get('first') == 'scooby'
	names[1].get('last') == 'gilmore'
	r2.get("namesToAges") == [scooby: 10, fred: 25]
    }

    def "test enum fixed"() {
	setup:
	def schema = loadSchema("EnumFixed.json")
	def md5 = (0..15).collect { it } as byte[]
	def record = new GenericRecordBuilder(schema).tap {
	    set "suit", "HEARTS"
	    set "md5", md5
	}.build()

	when:
	def map = toMap(record)

	then:
	map.suit == "HEARTS"
	map.md5 == md5
    }

    def "test enum fixed reverse"() {
	setup:
	def schema = loadSchema("EnumFixed.json")
	def md5 = (0..15).collect { it } as byte[]
	def map = [suit: 'HEARTS', md5: md5]

	when:
	def record = toRecord(schema, map)

	then:
	record.get('suit') == "HEARTS"
	record.get('md5') == md5
    }

    def "test logical"() {
	setup:
	def schema = loadSchema("Logical.json")
	def now = Instant.now()
	def uuid = UUID.randomUUID()
	def record = new GenericRecordBuilder(schema).tap {
	    set "inMillis", now
	    set "id", uuid
	}.build()

	when:
	def map = toMap(record)

	then:
	map.inMillis == now
	map.id == uuid
    }

    def "test logical reverse"() {
	setup:
	def schema = loadSchema("Logical.json")
	def now = Instant.now()
	def uuid = UUID.randomUUID()
	def map = [inMillis: now, id: uuid]
	
	when:
	def record = toRecord(schema, map)

	then:
	record.get('inMillis') == now
	record.get('id') == uuid
    }
}
