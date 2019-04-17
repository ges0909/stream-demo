package schrader.stream.test;

import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.*;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamTest {

    /**
     * Stream creation
     */

    @Test
    public void emptyStream() {
        final Stream<String> s = Stream.empty();
        assertThat(s.count()).isEqualTo(0);
    }

    @Test
    public void emptyStreamWithOf() {
        final Stream<String> s = Stream.of();
        assertThat(s.count()).isEqualTo(0);
    }

    @Test
    public void streamFromIntegers() {
        final Stream<Integer> s = Stream.of(1, 2, 3);
        assertThat(s.toArray(Integer[]::new)).isEqualTo(new Integer[]{1, 2, 3});
    }

    @Test
    public void streamFromCharacters() {
        final Stream<Character> s = Stream.of('a', 'b', 'c');
        assertThat(s.toArray(Character[]::new)).isEqualTo(new Character[]{'a', 'b', 'c'});
    }

    @Test
    public void streamFromStrings() {
        final Stream<String> s = Stream.of("a", "b", "c");
        assertThat(s.toArray(String[]::new)).isEqualTo(new String[]{"a", "b", "c"});
    }

    @Test
    public void streamFromIntPrimitives() {
        final IntStream s = "test".chars();
        final IntStream s2 = "test".codePoints();
        assertThat(s.toArray()).isEqualTo(new int[]{116, 101, 115, 116});
        assertThat(s2.toArray()).isEqualTo(new int[]{116, 101, 115, 116});
    }

    @Test
    public void streamFromPattern() {
        final Pattern p = Pattern.compile(",");
        final Stream<String> s = p.splitAsStream("a,b,c");
        assertThat(s.toArray(String[]::new)).isEqualTo(new String[]{"a", "b", "c"});
    }

    @Test
    public void streamFromArray() {
        final Stream<Integer> s = Stream.of(1, 2, 3);
        assertThat(s.toArray(Integer[]::new)).isEqualTo(new Integer[]{1, 2, 3});
    }

    @Test
    public void streamFromArrayOfIntPrimitives() {
        IntStream s = Arrays.stream(new int[]{1, 2, 3});
        assertThat(s.toArray()).isEqualTo(new Integer[]{1, 2, 3});
    }

    @Test
    public void streamFromArrayOfLongPrimitives() {
        final LongStream s = Arrays.stream(new long[]{1, 2, 3});
        assertThat(s.toArray()).isEqualTo(new Long[]{1L, 2L, 3L});
    }

    @Test
    public void streamFromArrayOfDoublePrimitives() {
        final DoubleStream s = Arrays.stream(new double[]{1, 2, 3});
        assertThat(s.toArray()).isEqualTo(new Double[]{1.0, 2.0, 3.0});
    }

    @Test
    public void streamFromCollections() {
        final List<Integer> l = Arrays.asList(1, 2, 3);
        final Stream<Integer> s = l.stream(); // any type of collection possible
        assertThat(s.toArray()).isEqualTo(new Integer[]{1, 2, 3});
    }

    @Test
    public void parallelStreamFromCollections() {
        final Set<Integer> s = new HashSet<>(Arrays.asList(1, 2, 3));
        final Stream<Integer> ps = s.parallelStream(); // any type of collection possible
        assertThat(ps.toArray()).isEqualTo(new Integer[]{1, 2, 3});
    }

    @Test
    public void streamFromGenerate() {
        var s = Stream.generate(new Random()::nextInt).limit(6); // generates an infinite unordered stream
        s.forEach(System.out::println);
    }

    @Test
    public void streamFromIterate() {
        var s = Stream.iterate(3, n -> n + 1).limit(6); // generates an infinite ordered stream
        assertThat(s.toArray()).isEqualTo(new int[]{3, 4, 5, 6, 7, 8});
    }

    @Test
    public void streamFromFile() throws IOException, URISyntaxException {
        try (final Stream<String> s = Files.lines(Paths.get(getClass().getClassLoader().getResource("samples.txt").toURI()))) {
            assertThat(s.toArray()).isEqualTo(new String[]{"eins", "zwei", "drei"});
        }
    }

    /**
     * Stream conversion
     */

    @Test
    public void streamToList() {
        final Stream<Integer> s = Stream.of(1, 2, 3);
        final List<Integer> list = s.collect(Collectors.toList());
        assertThat(list).isEqualTo(Arrays.asList(1, 2, 3));
    }

    @Test
    public void streamToCollection() {
        final Stream<Integer> s = Stream.of(1, 2, 3);
        final Collection<Integer> collection = s.collect(Collectors.toCollection(ArrayList::new));
        assertThat(collection).isEqualTo(Arrays.asList(1, 2, 3));
    }

    @Test
    public void streamToListWithForEach() {
        final Stream<Integer> s = Stream.of(1, 2, 3);
        final List<Integer> list = new ArrayList<>();
        s.forEach(list::add);
        assertThat(list).isEqualTo(Arrays.asList(1, 2, 3));
    }

    @Test
    public void streamToMap() {
        final Stream<String[]> s = Stream.of(new String[][]{{"1", "one"}, {"2", "two"}});
        final Map<String, String> map = s.collect(Collectors.toMap(e -> e[0], e -> e[1]));
        assertThat(map).isEqualTo(new HashMap<>() {{
            put("1", "one");
            put("2", "two");
        }});
    }

    @Test
    public void streamToArray() {
        Stream<Integer> s = Stream.of(1, 2, 3);
        Integer[] array = s.toArray(Integer[]::new);
        assertThat(array).isEqualTo(new Integer[]{1, 2, 3});
    }

    @Test
    public void streamToArrayOfIntPrimitives() {
        final Stream<Integer> s = Stream.of(1, 2, 3);
        final int[] array = s.mapToInt(i -> i).toArray();
        assertThat(array).isEqualTo(new Integer[]{1, 2, 3});
    }

    @Test
    public void reduceListOfElementsToString() {
        final List<Integer> l = Arrays.asList(1, 2, 3);
        final String s = l.stream().map(String::valueOf).reduce((a, b) -> a + ", " + b).orElseGet(String::new);
        assertThat(s).isEqualTo("1, 2, 3");
    }

    @Test
    public void convertListToArray() {
        final List<Integer> l = Arrays.asList(1, 2, 3);
        final Integer[] a = l.stream().toArray(Integer[]::new);
        assertThat(a).isEqualTo(new Integer[]{1, 2, 3});
    }

    @Test
    public void convertListToIntArray() {
        final List<Integer> l = Arrays.asList(1, 2, 3);
        final int[] a = l.stream().mapToInt(i -> i).toArray();
        assertThat(a).isEqualTo(new Integer[]{1, 2, 3});
    }

    /**
     * Stream operation
     */

    public void filter() {
    }

    public void map() {
    }

    @Test
    public void sorted() {
        final List<Integer> list = Arrays.asList(2, 3, 1);
        final List<Integer> sortedList = list.stream().sorted().collect(Collectors.toList());
        assertThat(sortedList).isEqualTo(Arrays.asList(1, 2, 3));
    }

    @Test
    public void sortedWithComparator() {
        final List<Integer> list = Arrays.asList(2, 3, 1);
        final List<Integer> sortedList = list.stream().sorted(Comparator.comparing(Integer::intValue)).collect(Collectors.toList());
        assertThat(sortedList).isEqualTo(Arrays.asList(1, 2, 3));
    }

    public void forEach() {
    }

    public void reduce() {
    }

    public void count() {
    }

    public void collect() {
    }

    @Test
    public void allMatch() {
        Stream<String> s = Stream.of("eins", "zwei", "drei");
        boolean b = s.allMatch(v -> v.contains("ei"));
        assertThat(b).isTrue();
    }

    @Test
    public void anyMatch() {
        Stream<String> s = Stream.of("eins", "zwei", "drei");
        boolean b = s.anyMatch(v -> v.contains("dr"));
        assertThat(b).isTrue();
    }

    @Test
    public void noneMatch() {
        Stream<String> s = Stream.of("eins", "zwei", "drei");
        boolean b = s.noneMatch(v -> v.contains("ddr"));
        assertThat(b).isTrue();
    }

    @Test
    public void groupingBy() {
        final List<Pair> pairs = Arrays.asList(new Pair(1, "A"), new Pair(1, "B"), new Pair(2, "C"), new Pair(3, "D"));
        final Map<Integer, List<Pair>> groupedBy = pairs.stream().collect(Collectors.groupingBy(Pair::getId));
        assertThat(groupedBy.size()).isEqualTo(3);
        assertThat(groupedBy.get(1).size()).isEqualTo(2);
        assertThat(groupedBy.get(2).size()).isEqualTo(1);
        assertThat(groupedBy.get(3).size()).isEqualTo(1);
    }

    @Test
    public void partition() {
        final int size = 2;
        final AtomicInteger counter = new AtomicInteger(0);
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        final Collection<List<Integer>> actual = list.stream().collect(Collectors.groupingBy(it -> counter.getAndIncrement() / size)).values();
        assertThat(actual.size()).isEqualTo(3);
        assertThat(new ArrayList<>(actual).get(0).size()).isEqualTo(2);
        assertThat(new ArrayList<>(actual).get(1).size()).isEqualTo(2);
        assertThat(new ArrayList<>(actual).get(2).size()).isEqualTo(1);
    }

    @Test
    public void average() {
        long jan16 = LocalDate.of(2016, Month.JANUARY, 31).atTime(LocalTime.MAX).atZone(ZoneId.systemDefault()).toInstant()
                .toEpochMilli();
        long jan17 = LocalDate.of(2017, Month.JANUARY, 31).atTime(LocalTime.MAX).atZone(ZoneId.systemDefault()).toInstant()
                .toEpochMilli();
        long jan18 = LocalDate.of(2018, Month.JANUARY, 31).atTime(LocalTime.MAX).atZone(ZoneId.systemDefault()).toInstant()
                .toEpochMilli();
        long feb16 = LocalDate.of(2016, Month.FEBRUARY, 29).atTime(LocalTime.MAX).atZone(ZoneId.systemDefault()).toInstant()
                .toEpochMilli();
        long feb17 = LocalDate.of(2017, Month.FEBRUARY, 28).atTime(LocalTime.MAX).atZone(ZoneId.systemDefault()).toInstant()
                .toEpochMilli();
        long feb18 = LocalDate.of(2018, Month.FEBRUARY, 28).atTime(LocalTime.MAX).atZone(ZoneId.systemDefault()).toInstant()
                .toEpochMilli();

        Point[] _points = {new Point(jan16, 1.0), new Point(feb16, 2.0), new Point(jan17, 3.0), new Point(feb17, 4.0),
                new Point(jan18, 5.0), new Point(feb18, 6.0)};
        List<Point> points = Arrays.asList(_points);

        Map<Integer, Double> avgMap = new HashMap<>();
        Map<Integer, List<Point>> sortedByMonth = points.stream().collect(Collectors.groupingBy(this::month));
        for (Map.Entry<Integer, List<Point>> entry : sortedByMonth.entrySet()) {
            double avg = entry.getValue().stream().mapToDouble(Point::getValue).average().getAsDouble();
            avgMap.put(entry.getKey(), avg);
        }
    }

    /**
     * Java 9: takeWhile, dropWhile
     */

    @Test
    public void takeWhile() {
        final Integer[] array = Stream.of(0, 2, 5, 6, 8).takeWhile(n -> n % 2 == 0).toArray(Integer[]::new);
        assertThat(array).isEqualTo(new Integer[]{0, 2});
    }

    @Test
    public void dropWhile() {
        final Integer[] array = Stream.of(0, 2, 5, 6, 8).dropWhile(n -> n % 2 == 0).toArray(Integer[]::new);
        assertThat(array).isEqualTo(new Integer[]{5, 6, 8});
    }

    /*
     * Returns the number of the month ranging from 0..11.
     */
    private int month(Point point) {
        final Instant instant = Instant.ofEpochMilli(point.timestamp);
        final LocalDateTime dateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
        return dateTime.getMonth().ordinal();
    }

    class Pair {
        int id;
        String name;

        Pair(int id, String name) {
            this.id = id;
            this.name = name;
        }

        int getId() {
            return this.id;
        }
    }

    class Point {
        long timestamp;
        double value;

        Point(long timestamp, double value) {
            this.timestamp = timestamp;
            this.value = value;
        }

        double getValue() {
            return this.value;
        }
    }
}
