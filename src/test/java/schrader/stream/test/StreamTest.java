package schrader.stream.test;

import org.javatuples.Pair;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BinaryOperator;
import java.util.regex.Pattern;
import java.util.stream.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.fail;

@DisplayName("Stream demo tests")
class StreamTest {

    @Nested
    class StreamCreationTest {

        @Test
        void empty() {
            final Stream<String> s = Stream.empty();
            assertThat(s).isEmpty();
        }

        @Test
        void emptyWithOf() {
            final Stream<String> s = Stream.of();
            assertThat(s).isEmpty();
        }

        @Test
        void ofInteger() {
            final Stream<Integer> s = Stream.of(1, 2, 3);
            assertThat(s).containsExactly(1, 2, 3);
        }

        @Test
        void ofCharacter() {
            final Stream<Character> s = Stream.of('a', 'b', 'c');
            assertThat(s).containsExactly('a', 'b', 'c');
        }

        @Test
        void ofString() {
            final Stream<String> s = Stream.of("a", "b", "c");
            assertThat(s).containsExactly("a", "b", "c");
        }

        @Test
        void ofChars() {
            final IntStream s = "test".chars();
            assertThat(s).containsExactly(116, 101, 115, 116);
        }

        @Test
        void ofCodePoints() {
            final IntStream s = "test".chars();
            assertThat(s).containsExactly(116, 101, 115, 116);
        }

        @Test
        void ofPattern() {
            final Pattern p = Pattern.compile(",");
            final Stream<String> s = p.splitAsStream("a,b,c");
            assertThat(s).containsExactly("a", "b", "c");
        }

        @Test
        void ofArray() {
            final Integer[] a = {1, 2, 3};
            final Stream<Integer> s = Stream.of(a);
            assertThat(s).containsExactly(1, 2, 3);
        }

        @Test
        void ofIntArray() {
            IntStream s = Arrays.stream(new int[]{1, 2, 3});
            assertThat(s).containsExactly(1, 2, 3);
        }

        @Test
        void ofLongArray() {
            final LongStream s = Arrays.stream(new long[]{1, 2, 3});
            assertThat(s).containsExactly(1L, 2L, 3L);
        }

        @Test
        void ofDoubleArray() {
            final DoubleStream s = Arrays.stream(new double[]{1, 2, 3});
            assertThat(s).containsExactly(1.0, 2.0, 3.0);
        }

        @Test
        void ofCollection() {
            final List<Integer> l = List.of(1, 2, 3); // any type of collection possible
            final Stream<Integer> s = l.stream();
            assertThat(s).containsExactly(1, 2, 3);
        }

        @Test
        void ofCollectionParallel() {
            final Set<Integer> _s = Set.of(1, 2, 3);
            final Stream<Integer> s = _s.parallelStream();
            assertThat(s).containsExactlyInAnyOrder(1, 2, 3);
        }

        @Test
        void ofFile() throws IOException, URISyntaxException {
            final URI uri = getClass().getClassLoader().getResource("samples.txt").toURI();
            try (final Stream<String> lines = Files.lines(Paths.get(uri))) {
                assertThat(lines).containsExactly("one", "two", "three");
            }
        }

        @Test
        void streamBuilder() {
            final Stream<String> s = Stream.<String>builder().add("one").add("two").add("three").build();
            assertThat(s).containsExactly("one", "two", "three");
        }

        @Test
        void generate() {
            final var s = Stream.generate(new Random()::nextInt).limit(6); // generates an infinite unordered stream
        }

        @Test
        void iterate() {
            final var s = Stream.iterate(3, n -> n + 1).limit(6); // generates an infinite ordered stream
            assertThat(s).containsExactly(3, 4, 5, 6, 7, 8);
        }
    }

    @Nested
    class StreamPrimitiveTest {

        @Test
        void intStreamOf() {
            final var s = IntStream.of(1, 2, 3);
            assertThat(s).containsExactly(1, 2, 3);
        }

        @Test
        void range() {
            final var s = IntStream.range(1, 4);
            assertThat(s).containsExactly(1, 2, 3);
        }

        @Test
        void rangeClosed() {
            final var s = IntStream.rangeClosed(1, 4);
            assertThat(s).containsExactly(1, 2, 3, 4);
        }
    }

    @Nested
    class StreamConversionTest {

        @Test
        void toCollection() {
            final Stream<Integer> s = Stream.of(1, 2, 3);
            final Collection<Integer> c = s.collect(Collectors.toCollection(ArrayList::new));
            assertThat(c).containsExactly(1, 2, 3);
        }

        @Test
        void toList() {
            final Stream<Integer> s = Stream.of(1, 2, 3);
            final List<Integer> l = s.collect(Collectors.toList());
            assertThat(l).containsExactly(1, 2, 3);
        }

        @Test
        void toListForEach() {
            final Stream<Integer> s = Stream.of(1, 2, 3);
            final List<Integer> l = new ArrayList<>();
            s.forEach(l::add);
            assertThat(l).containsExactly(1, 2, 3);
        }

        @Test
        void toArray() {
            final Integer[] a = Stream.of(1, 2, 3).toArray(Integer[]::new);
            assertThat(a).containsExactly(1, 2, 3);
        }

        @Test
        void mapToInt() {
            final int[] a = Stream.of(1, 2, 3).mapToInt(i -> i).toArray();
            assertThat(a).containsExactly(1, 2, 3);
        }

        @Test
        void mapToLong() {
            final long[] a = Stream.of(1, 2, 3).mapToLong(l -> l).toArray();
            assertThat(a).isEqualTo(new long[]{1, 2, 3});
        }

        @Test
        void mapToDouble() {
            final double[] a = Stream.of(1, 2, 3).mapToDouble(d -> d).toArray();
            assertThat(a).containsExactly(1, 2, 3);
        }

        @Test
        void toMap() {
            final Stream<String[]> s = Stream.of(new String[][]{{"1", "one"}, {"2", "two"}, {"3", "three"}});
            final Map<String, String> m = s.collect(Collectors.toMap(e -> e[0], e -> e[1]));
            assertThat(m).isEqualTo(Map.of("1", "one", "2", "two", "3", "three"));
        }

        @Test
        void reduceToString() {
            final String s = Stream.of(1, 2, 3).map(String::valueOf).reduce((a, b) -> a + ", " + b).orElseGet(String::new);
            assertThat(s).isEqualTo("1, 2, 3");
        }
    }

    @Nested
    class StreamOperationTest {

        @Test
        void findFirst() {
            Optional<Integer> o = Stream.of(1, 2, 3).findFirst();
            o.ifPresentOrElse(
                    i -> assertThat(i).isEqualTo(1),
                    () -> fail("optional is empty"));
        }

        @Test
        void findAny() {
            Optional<Integer> o = Stream.of(1, 2, 3).findAny();
            o.ifPresentOrElse(
                    i -> assertThat(List.of(i)).containsAnyOf(1, 2, 3),
                    () -> fail("optional is empty"));
        }

        @Test
        void forEach() {
        }

        @Test
        void filter() {
            int[] a = Stream.of(1, 2, 3).filter(i -> i % 2 == 0).mapToInt(Integer::intValue).toArray();
            assertThat(a).containsExactly(2);
        }

        @Test
        void map() {
            int[] a = Stream.of("one", "two", "three").map(String::length).mapToInt(Integer::intValue).toArray();
            assertThat(a).containsExactly(3, 3, 5);
        }

        @Test
        void sorted() {
            final var l = Stream.of(2, 3, 1).sorted().collect(Collectors.toList());
            assertThat(l).containsExactly(1, 2, 3);
        }

        @Test
        void sortedWithComparator() {
            final List<Integer> l = Stream.of(2, 3, 1)
                    .sorted(Comparator.comparing(Integer::intValue))
                    .collect(Collectors.toList());
            assertThat(l).containsExactly(1, 2, 3);
        }

        @Test
        void reduce() {
            final BinaryOperator<Integer> summing = (a, b) -> a + b;
            final int sum = Stream.of(1, 2, 3).reduce(1 /*seed*/, summing);
            assertThat(sum).isEqualTo(7);
        }

        @Test
        void reduceWithIntegerSum() {
            final int sum = Stream.of(1, 2, 3).reduce(1 /*seed*/, Integer::sum);
            assertThat(sum).isEqualTo(7);
        }

        @Test
        void sum() {
            final int sum = Stream.of(1, 2, 3).mapToInt(Integer::intValue).sum();
            assertThat(sum).isEqualTo(6);
        }

        @Test
        void count() {
            final long count = Stream.of(1, 2, 3).count();
            assertThat(count).isEqualTo(3);
        }

        @Test
        void average() {
            final OptionalDouble o = Stream.of(1, 2, 3).mapToDouble(d -> d).average();
            o.ifPresentOrElse(
                    avg -> assertThat(avg).isEqualTo(2.0),
                    () -> fail("empty optional double"));
        }

        @Test
        void collect() {
        }

        @Test
        void limit() {
            Stream<Integer> s = Stream.of(1, 2, 3).limit(1);
            assertThat(s).containsOnly(1);
        }

        @Test
        void allMatch() {
            Stream<String> s = Stream.of("eins", "zwei", "drei");
            boolean result = s.allMatch(v -> v.contains("ei"));
            assertThat(result).isTrue();
        }

        @Test
        void anyMatch() {
            Stream<String> s = Stream.of("eins", "zwei", "drei");
            boolean result = s.anyMatch(v -> v.contains("dr"));
            assertThat(result).isTrue();
        }

        @Test
        void noneMatch() {
            Stream<String> s = Stream.of("eins", "zwei", "drei");
            boolean result = s.noneMatch(v -> v.contains("ddr"));
            assertThat(result).isTrue();
        }

        @Test
        void groupingBy() {
            final List<Pair<Integer, String>> pairs = List.of(
                    new Pair<>(1, "A"),
                    new Pair<>(1, "B"),
                    new Pair<>(2, "C"),
                    new Pair<>(3, "D"));
            final Map<Integer, List<Pair<Integer, String>>> groupedBy = pairs.stream()
                    .collect(Collectors.groupingBy(Pair::getValue0));
            assertThat(groupedBy.get(1)).containsExactly(pairs.get(0), pairs.get(1));
            assertThat(groupedBy.get(2)).containsExactly(pairs.get(2));
            assertThat(groupedBy.get(3)).containsExactly(pairs.get(3));
        }

        @Test
        void partitioningByNumber() {
            final AtomicInteger number = new AtomicInteger(0);
            final Collection<List<Integer>> partitionedCollection = Stream.of(1, 2, 3, 4, 5)
                    .collect(Collectors.groupingBy(i -> number.getAndIncrement() / 2))
                    .values();
            final List<List<Integer>> partitionedList = new ArrayList<>(partitionedCollection);
            assertThat(partitionedList.get(0)).containsExactly(1, 2);
            assertThat(partitionedList.get(1)).containsExactly(3, 4);
            assertThat(partitionedList.get(2)).containsExactly(5);
        }
    }

    @Nested
    @DisplayName("Java 9 add-ons")
    class Java9StreamTest {

        @Test
        void takeWhile() {
            final Integer[] array = Stream.of(0, 2, 5, 6, 8).takeWhile(n -> n % 2 == 0).toArray(Integer[]::new);
            assertThat(array).containsExactly(0, 2);
        }

        @Test
        void dropWhile() {
            final Integer[] array = Stream.of(0, 2, 5, 6, 8).dropWhile(n -> n % 2 == 0).toArray(Integer[]::new);
            assertThat(array).containsExactly(5, 6, 8);
        }
    }
}
