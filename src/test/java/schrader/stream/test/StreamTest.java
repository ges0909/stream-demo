package schrader.stream.test;

import org.javatuples.Pair;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.fail;

class StreamTest {

    @Nested
    class StreamCreationTest {

        @Test
        void emptyStream() {
            final Stream<String> s = Stream.empty();
            assertThat(s.count()).isEqualTo(0);
        }

        @Test
        void emptyStreamWithOf() {
            final Stream<String> s = Stream.of();
            assertThat(s.count()).isEqualTo(0);
        }

        @Test
        void streamFromIntegers() {
            final Stream<Integer> s = Stream.of(1, 2, 3);
            assertThat(s.toArray(Integer[]::new)).isEqualTo(new Integer[]{1, 2, 3});
        }

        @Test
        void streamFromCharacters() {
            final Stream<Character> s = Stream.of('a', 'b', 'c');
            assertThat(s.toArray(Character[]::new)).isEqualTo(new Character[]{'a', 'b', 'c'});
        }

        @Test
        void streamFromStrings() {
            final Stream<String> s = Stream.of("a", "b", "c");
            assertThat(s.toArray(String[]::new)).isEqualTo(new String[]{"a", "b", "c"});
        }

        @Test
        void streamFromIntPrimitives() {
            final IntStream s = "test".chars();
            final IntStream s2 = "test".codePoints();
            assertThat(s.toArray()).isEqualTo(new int[]{116, 101, 115, 116});
            assertThat(s2.toArray()).isEqualTo(new int[]{116, 101, 115, 116});
        }

        @Test
        void streamFromPattern() {
            final Pattern p = Pattern.compile(",");
            final Stream<String> s = p.splitAsStream("a,b,c");
            assertThat(s.toArray(String[]::new)).isEqualTo(new String[]{"a", "b", "c"});
        }

        @Test
        void streamFromArray() {
            final Stream<Integer> s = Stream.of(1, 2, 3);
            assertThat(s.toArray(Integer[]::new)).isEqualTo(new Integer[]{1, 2, 3});
        }

        @Test
        void streamFromArrayOfIntPrimitives() {
            IntStream s = Arrays.stream(new int[]{1, 2, 3});
            assertThat(s.toArray()).isEqualTo(new Integer[]{1, 2, 3});
        }

        @Test
        void streamFromArrayOfLongPrimitives() {
            final LongStream s = Arrays.stream(new long[]{1, 2, 3});
            assertThat(s.toArray()).isEqualTo(new Long[]{1L, 2L, 3L});
        }

        @Test
        void streamFromArrayOfDoublePrimitives() {
            final DoubleStream s = Arrays.stream(new double[]{1, 2, 3});
            assertThat(s.toArray()).isEqualTo(new Double[]{1.0, 2.0, 3.0});
        }

        @Test
        void streamFromCollections() {
            final List<Integer> l = Arrays.asList(1, 2, 3);
            final Stream<Integer> s = l.stream(); // any type of collection possible
            assertThat(s.toArray()).isEqualTo(new Integer[]{1, 2, 3});
        }

        @Test
        void parallelStreamFromCollections() {
            final Set<Integer> s = new HashSet<>(Arrays.asList(1, 2, 3));
            final Stream<Integer> ps = s.parallelStream(); // any type of collection possible
            assertThat(ps.toArray()).isEqualTo(new Integer[]{1, 2, 3});
        }

        @Test
        void streamFromGenerate() {
            var s = Stream.generate(new Random()::nextInt).limit(6); // generates an infinite unordered stream
            s.forEach(System.out::println);
        }

        @Test
        void streamFromIterate() {
            var s = Stream.iterate(3, n -> n + 1).limit(6); // generates an infinite ordered stream
            assertThat(s.toArray()).isEqualTo(new int[]{3, 4, 5, 6, 7, 8});
        }

        @Test
        void streamFromFile() throws IOException, URISyntaxException {
            URI uri = getClass().getClassLoader().getResource("samples.txt").toURI();
            try (final Stream<String> lines = Files.lines(Paths.get(uri))) {
                assertThat(lines.toArray()).isEqualTo(new String[]{"one", "two", "three"});
            }
        }
    }

    @Nested
    class IntStreamTest {

        @Test
        void range() {
            var s = IntStream.range(1, 4);
            assertThat(s.toArray()).isEqualTo(new int[]{1, 2, 3});
        }

        @Test
        void rangeClosed() {
            var s = IntStream.rangeClosed(1, 4);
            assertThat(s.toArray()).isEqualTo(new int[]{1, 2, 3, 4});
        }
    }

    @Nested
    class StreamConversionTest {
        @Test
        void streamToList() {
            final Stream<Integer> s = Stream.of(1, 2, 3);
            final List<Integer> l = s.collect(Collectors.toList());
            assertThat(l).isEqualTo(List.of(1, 2, 3));
        }

        @Test
        void streamToCollection() {
            final Stream<Integer> s = Stream.of(1, 2, 3);
            final Collection<Integer> c = s.collect(Collectors.toCollection(ArrayList::new));
            assertThat(c).isEqualTo(List.of(1, 2, 3));
        }

        @Test
        void streamToListWithForEach() {
            final Stream<Integer> s = Stream.of(1, 2, 3);
            final List<Integer> l = new ArrayList<>();
            s.forEach(l::add);
            assertThat(l).isEqualTo(List.of(1, 2, 3));
        }

        @Test
        void streamToMap() {
            final Stream<String[]> s = Stream.of(new String[][]{{"1", "one"}, {"2", "two"}});
            final Map<String, String> m = s.collect(Collectors.toMap(e -> e[0], e -> e[1]));
            assertThat(m).isEqualTo(new HashMap<>() {{
                put("1", "one");
                put("2", "two");
            }});
        }

        @Test
        void streamToArray() {
            Integer[] a = Stream.of(1, 2, 3).toArray(Integer[]::new);
            assertThat(a).isEqualTo(new Integer[]{1, 2, 3});
        }

        @Test
        void streamToArrayOfIntPrimitives() {
            final int[] a = Stream.of(1, 2, 3).mapToInt(i -> i).toArray();
            assertThat(a).isEqualTo(new Integer[]{1, 2, 3});
        }

        @Test
        void reduceStreamToString() {
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
                    v -> assertThat(v).isEqualTo(1),
                    () -> fail("optional is empty"));
        }

        @Test
        void findAny() {
            Optional<Integer> o = Stream.of(1, 2, 3).findAny();
            o.ifPresentOrElse(
                    v -> assertThat(List.of(v)).containsAnyOf(1, 2, 3),
                    () -> fail("optional is empty"));
        }

        @Test
        void forEach() {
        }

        @Test
        void filter() {
            int[] arr = Stream.of(1, 2, 3).filter(i -> i % 2 == 0).mapToInt(Integer::intValue).toArray();
            assertThat(arr).isEqualTo(new int[]{2});
        }

        @Test
        void map() {
            int[] arr = Stream.of("one", "two", "three").map(String::length).mapToInt(Integer::intValue).toArray();
            assertThat(arr).isEqualTo(new int[]{3, 3, 5});
        }

        @Test
        void sorted() {
            final var sortedList = Stream.of(2, 3, 1).sorted().collect(Collectors.toList());
            assertThat(sortedList).isEqualTo(Arrays.asList(1, 2, 3));
        }

        @Test
        void sortedWithComparator() {
            final var sortedList = Stream.of(2, 3, 1).sorted(Comparator.comparing(Integer::intValue)).collect(Collectors.toList());
            assertThat(sortedList).isEqualTo(Arrays.asList(1, 2, 3));
        }

        @Test
        void reduce() {
            BiFunction<Integer, Integer, Integer> summe = (a, b) -> a + b;
            Integer sum = Stream.of(1, 2, 3).reduce(1 /*seed*/, (a, b) -> a + b);
            assertThat(sum).isEqualTo(7);
        }

        @Test
        void reduceWithSum() {
            Integer sum = Stream.of(1, 2, 3).reduce(1 /*seed*/, Integer::sum);
            assertThat(sum).isEqualTo(7);
        }

        @Test
        void sumOnIntStream() {
            Integer sum = Stream.of(1, 2, 3).mapToInt(Integer::intValue).sum();
            assertThat(sum).isEqualTo(6);
        }

        @Test
        void count() {
            var count = Stream.of(1, 2, 3).count();
            assertThat(count).isEqualTo(3);
        }

        @Test
        void average() {
            OptionalDouble avg = Stream.of(1, 2, 3).mapToInt(Integer::intValue).average();
            avg.ifPresentOrElse(v -> assertThat(v).isEqualTo(2.0), () -> fail("empty optional double"));
        }

        @Test
        void collect() {
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
            final Map<Integer, List<Pair<Integer, String>>> groupedBy = pairs.stream().collect(Collectors.groupingBy(Pair::getValue0));
            assertThat(groupedBy.get(1)).isEqualTo(List.of(pairs.get(0), pairs.get(1)));
            assertThat(groupedBy.get(2)).isEqualTo(List.of(pairs.get(2)));
            assertThat(groupedBy.get(3)).isEqualTo(List.of(pairs.get(3)));
        }

        @Test
        void partitioningByNumber() {
            final AtomicInteger number = new AtomicInteger(0);
            final Collection<List<Integer>> partitionedCollection = Stream.of(1, 2, 3, 4, 5)
                    .collect(Collectors.groupingBy(i -> number.getAndIncrement() / 2))
                    .values();
            final List<List<Integer>> partitionedList = new ArrayList<>(partitionedCollection);
            assertThat(partitionedList.get(0)).isEqualTo(List.of(1, 2));
            assertThat(partitionedList.get(1)).isEqualTo(List.of(3, 4));
            assertThat(partitionedList.get(2)).isEqualTo(List.of(5));
        }
    }

    @Nested
    class Java9StreamTest {

        @Test
        void takeWhile() {
            final Integer[] array = Stream.of(0, 2, 5, 6, 8).takeWhile(n -> n % 2 == 0).toArray(Integer[]::new);
            assertThat(array).isEqualTo(new Integer[]{0, 2});
        }

        @Test
        void dropWhile() {
            final Integer[] array = Stream.of(0, 2, 5, 6, 8).dropWhile(n -> n % 2 == 0).toArray(Integer[]::new);
            assertThat(array).isEqualTo(new Integer[]{5, 6, 8});
        }
    }
}
