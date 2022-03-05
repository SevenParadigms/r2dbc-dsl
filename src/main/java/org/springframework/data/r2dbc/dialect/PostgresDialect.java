package org.springframework.data.r2dbc.dialect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.r2dbc.postgresql.codec.Json;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.r2dbc.config.Beans;
import org.springframework.data.r2dbc.config.ExpressionParserCache;
import org.springframework.data.r2dbc.support.JsonUtils;
import org.springframework.data.relational.core.dialect.ArrayColumns;
import org.springframework.data.util.Lazy;
import org.springframework.expression.Expression;
import org.springframework.lang.NonNull;
import org.springframework.r2dbc.core.binding.BindMarkersFactory;
import org.springframework.util.ClassUtils;

import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * An SQL dialect for Postgres.
 *
 * @author Mark Paluch
 * @author Jose Luis Leon
 */
public class PostgresDialect extends org.springframework.data.relational.core.dialect.PostgresDialect
		implements R2dbcDialect {

	private static final Set<Class<?>> SIMPLE_TYPES;

	private static final boolean EXPRESSION_PRESENT = ClassUtils.isPresent("org.springframework.expression.Expression",
			PostgresDialect.class.getClassLoader());

	private static final boolean JSON_PRESENT = ClassUtils.isPresent("io.r2dbc.postgresql.codec.Json",
			PostgresDialect.class.getClassLoader());

	private static final boolean GEO_TYPES_PRESENT = ClassUtils.isPresent("io.r2dbc.postgresql.codec.Polygon",
			PostgresDialect.class.getClassLoader());

	static {

		Set<Class<?>> simpleTypes = new HashSet<>(
				Arrays.asList(UUID.class, URL.class, URI.class, InetAddress.class, Map.class));

		// conditional Postgres Geo support.
		Stream.of("io.r2dbc.postgresql.codec.Box", //
				"io.r2dbc.postgresql.codec.Circle", //
				"io.r2dbc.postgresql.codec.Line", //
				"io.r2dbc.postgresql.codec.Lseg", //
				"io.r2dbc.postgresql.codec.Point", //
				"io.r2dbc.postgresql.codec.Path", //
				"io.r2dbc.postgresql.codec.Polygon") //
				.forEach(s -> ifClassPresent(s, simpleTypes::add));

		// conditional Postgres JSON support.
		ifClassPresent("io.r2dbc.postgresql.codec.Json", simpleTypes::add);

		// conditional Postgres Interval support
		ifClassPresent("io.r2dbc.postgresql.codec.Interval", simpleTypes::add);

		SIMPLE_TYPES = simpleTypes;
	}

	/**
	 * Singleton instance.
	 */
	public static final PostgresDialect INSTANCE = new PostgresDialect();

	private static final BindMarkersFactory INDEXED = BindMarkersFactory.indexed("$", 1);

	private final Lazy<ArrayColumns> arrayColumns = Lazy.of(() -> new R2dbcArrayColumns(
			org.springframework.data.relational.core.dialect.PostgresDialect.INSTANCE.getArraySupport(),
			getSimpleTypeHolder()));

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#getBindMarkersFactory()
	 */
	@Override
	public BindMarkersFactory getBindMarkersFactory() {
		return INDEXED;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#getSimpleTypesKeys()
	 */
	@Override
	public Collection<? extends Class<?>> getSimpleTypes() {
		return SIMPLE_TYPES;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#getArraySupport()
	 */
	@Override
	public ArrayColumns getArraySupport() {
		return this.arrayColumns.get();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#getConverters()
	 */
	@Override
	public Collection<Object> getConverters() {

		if (!GEO_TYPES_PRESENT && !JSON_PRESENT) {
			return Collections.emptyList();
		}

		List<Object> converters = new ArrayList<>();

		if (GEO_TYPES_PRESENT) {
			converters.addAll(Arrays.asList(FromPostgresPointConverter.INSTANCE, ToPostgresPointConverter.INSTANCE, //
					FromPostgresCircleConverter.INSTANCE, ToPostgresCircleConverter.INSTANCE, //
					FromPostgresBoxConverter.INSTANCE, ToPostgresBoxConverter.INSTANCE, //
					FromPostgresPolygonConverter.INSTANCE, ToPostgresPolygonConverter.INSTANCE));
		}

		if (JSON_PRESENT) {
			converters.addAll(Arrays.asList(JsonToByteArrayConverter.INSTANCE, JsonToStringConverter.INSTANCE));
			converters.addAll(Arrays.asList(JsonToNodeConverter.INSTANCE, NodeToJsonConverter.INSTANCE));
		}

		if (EXPRESSION_PRESENT) {
			converters.addAll(Arrays.asList(StringToExpressionConverter.INSTANCE, ExpressionToStringConverter.INSTANCE));
		}

		return converters;
	}

	private static class R2dbcArrayColumns implements ArrayColumns {

		private final ArrayColumns delegate;
		private final SimpleTypeHolder simpleTypeHolder;

		R2dbcArrayColumns(ArrayColumns delegate, SimpleTypeHolder simpleTypeHolder) {
			this.delegate = delegate;
			this.simpleTypeHolder = simpleTypeHolder;
		}

		@Override
		public boolean isSupported() {
			return this.delegate.isSupported();
		}

		@Override
		public Class<?> getArrayType(Class<?> userType) {

			Class<?> typeToUse = userType;
			while (typeToUse.getComponentType() != null) {
				typeToUse = typeToUse.getComponentType();
			}

			if (!this.simpleTypeHolder.isSimpleType(typeToUse)) {
				throw new IllegalArgumentException("Unsupported array type: " + ClassUtils.getQualifiedName(typeToUse));
			}

			return this.delegate.getArrayType(typeToUse);
		}
	}

	/**
	 * If the class is present on the class path, invoke the specified consumer {@code action} with the class object,
	 * otherwise do nothing.
	 *
	 * @param action block to be executed if a value is present.
	 */
	private static void ifClassPresent(String className, Consumer<Class<?>> action) {

		if (ClassUtils.isPresent(className, PostgresDialect.class.getClassLoader())) {
			action.accept(ClassUtils.resolveClassName(className, PostgresDialect.class.getClassLoader()));
		}
	}

	@ReadingConverter
	private enum FromPostgresBoxConverter implements Converter<io.r2dbc.postgresql.codec.Box, Box> {

		INSTANCE;

		@Override
		public Box convert(io.r2dbc.postgresql.codec.Box source) {
			return new Box(FromPostgresPointConverter.INSTANCE.convert(source.getA()),
					FromPostgresPointConverter.INSTANCE.convert(source.getB()));
		}
	}

	@WritingConverter
	private enum ToPostgresBoxConverter implements Converter<Box, io.r2dbc.postgresql.codec.Box> {

		INSTANCE;

		@Override
		public io.r2dbc.postgresql.codec.Box convert(Box source) {
			return io.r2dbc.postgresql.codec.Box.of(ToPostgresPointConverter.INSTANCE.convert(source.getFirst()),
					ToPostgresPointConverter.INSTANCE.convert(source.getSecond()));
		}
	}

	@ReadingConverter
	private enum FromPostgresCircleConverter implements Converter<io.r2dbc.postgresql.codec.Circle, Circle> {

		INSTANCE;

		@Override
		public Circle convert(io.r2dbc.postgresql.codec.Circle source) {
			return new Circle(source.getCenter().getX(), source.getCenter().getY(), source.getRadius());
		}
	}

	@WritingConverter
	private enum ToPostgresCircleConverter implements Converter<Circle, io.r2dbc.postgresql.codec.Circle> {

		INSTANCE;

		@Override
		public io.r2dbc.postgresql.codec.Circle convert(Circle source) {
			return io.r2dbc.postgresql.codec.Circle.of(source.getCenter().getX(), source.getCenter().getY(),
					source.getRadius().getValue());
		}
	}

	@ReadingConverter
	private enum FromPostgresPolygonConverter implements Converter<io.r2dbc.postgresql.codec.Polygon, Polygon> {

		INSTANCE;

		@Override
		public Polygon convert(io.r2dbc.postgresql.codec.Polygon source) {

			List<io.r2dbc.postgresql.codec.Point> sourcePoints = source.getPoints();
			List<Point> targetPoints = new ArrayList<>(sourcePoints.size());

			for (io.r2dbc.postgresql.codec.Point sourcePoint : sourcePoints) {
				targetPoints.add(FromPostgresPointConverter.INSTANCE.convert(sourcePoint));
			}

			return new Polygon(targetPoints);
		}
	}

	@WritingConverter
	private enum ToPostgresPolygonConverter implements Converter<Polygon, io.r2dbc.postgresql.codec.Polygon> {

		INSTANCE;

		@Override
		public io.r2dbc.postgresql.codec.Polygon convert(Polygon source) {

			List<Point> sourcePoints = source.getPoints();
			List<io.r2dbc.postgresql.codec.Point> targetPoints = new ArrayList<>(sourcePoints.size());

			for (Point sourcePoint : sourcePoints) {
				targetPoints.add(ToPostgresPointConverter.INSTANCE.convert(sourcePoint));
			}

			return io.r2dbc.postgresql.codec.Polygon.of(targetPoints);
		}
	}

	@ReadingConverter
	private enum FromPostgresPointConverter implements Converter<io.r2dbc.postgresql.codec.Point, Point> {

		INSTANCE;

		@Override
		@NonNull
		public Point convert(io.r2dbc.postgresql.codec.Point source) {
			return new Point(source.getX(), source.getY());
		}
	}

	@WritingConverter
	private enum ToPostgresPointConverter implements Converter<Point, io.r2dbc.postgresql.codec.Point> {

		INSTANCE;

		@Override
		@NonNull
		public io.r2dbc.postgresql.codec.Point convert(Point source) {
			return io.r2dbc.postgresql.codec.Point.of(source.getX(), source.getY());
		}
	}

	@WritingConverter
	private enum NodeToJsonConverter implements Converter<JsonNode, Json> {

		INSTANCE;

		public Json convert(JsonNode source) {
			return Json.of(source.toString());
		}
	}

	@ReadingConverter
	private enum JsonToNodeConverter implements Converter<Json, JsonNode> {

		INSTANCE;

		public JsonNode convert(Json json) {
			try {
				return JsonUtils.getMapper().readValue(json.asString(), new TypeReference<>() {
				});
			} catch (JsonProcessingException ex) {
				ex.printStackTrace();
			}
			return JsonUtils.getMapper().createObjectNode();
		}
	}

	@ReadingConverter
	private enum JsonToStringConverter implements Converter<Json, String> {

		INSTANCE;

		@Override
		@NonNull
		public String convert(Json source) {
			return source.asString();
		}
	}

	@ReadingConverter
	private enum JsonToByteArrayConverter implements Converter<Json, byte[]> {

		INSTANCE;

		@Override
		@NonNull
		public byte[] convert(Json source) {
			return source.asArray();
		}
	}

	@WritingConverter
	private enum ExpressionToStringConverter implements Converter<Expression, String> {

		INSTANCE;

		public String convert(Expression source) {
			return source.getExpressionString();
		}
	}

	@ReadingConverter
	private enum StringToExpressionConverter implements Converter<String, Expression> {

		INSTANCE;

		public Expression convert(String value) {
			var parserCache = Beans.of(ExpressionParserCache.class, new ExpressionParserCache());
			return parserCache.parseExpression(value);
		}
	}
}
