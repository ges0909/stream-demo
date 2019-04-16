package schrader.stream.test;

import org.junit.Test;

import java.io.IOException;
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
        Stream<String> s = Stream.of(); // Stream<T> not required because of Java 10 local variable type inference
        Stream<String> s2 = Stream.empty();
        assertThat(s.count()).isEqualTo(0);
        assertThat(s2.count()).isEqualTo(0);
    }

    @Test
    public void streamFromIntegers() {
        Stream<Integer> s = Stream.of(1, 2, 3);
        assertThat(s.toArray(Integer[]::new)).isEqualTo(new Integer[]{1, 2, 3});
    }

    @Test
    public void streamFromCharacters() {
        Stream<Character> s = Stream.of('a', 'b', 'c');
        assertThat(s.toArray(Character[]::new)).isEqualTo(new Character[]{'a', 'b', 'c'});
    }

    @Test
    public void streamFromStrings() {
        Stream<String> s = Stream.of("a", "b", "c");
        assertThat(s.toArray(String[]::new)).isEqualTo(new String[]{"a", "b", "c"});
    }

    @Test
    public void streamFromIntPrimitives() {
        IntStream s = "test".chars();
        IntStream s2 = "test".codePoints();
        assertThat(s.toArray()).isEqualTo(new int[]{116, 101, 115, 116});
        assertThat(s2.toArray()).isEqualTo(new int[]{116, 101, 115, 116});
    }

    @Test
    public void streamFromPattern() {
        Pattern p = Pattern.compile(",");
        Stream<String> s = p.splitAsStream("a,b,c");
        assertThat(s.toArray(String[]::new)).isEqualTo(new String[]{"a", "b", "c"});
    }

    @Test
    public void streamFromArray() {
        Stream<Integer> s = Stream.of(1, 2, 3);
        assertThat(s.toArray(Integer[]::new)).isEqualTo(new Integer[]{1, 2, 3});
    }

    @Test
    public void streamFromArrayOfIntPrimitives() {
        IntStream s = Arrays.stream(new int[]{1, 2, 3});
        assertThat(s.toArray()).isEqualTo(new Integer[]{1, 2, 3});
    }

    @Test
    public void streamFromArrayOfLongPrimitives() {
        LongStream s = Arrays.stream(new long[]{1, 2, 3});
        assertThat(s.toArray()).isEqualTo(new Long[]{1L, 2L, 3L});
    }

    @Test
    public void streamFromArrayOfDoublePrimitives() {
        DoubleStream s = Arrays.stream(new double[]{1, 2, 3});
        assertThat(s.toArray()).isEqualTo(new Double[]{1.0, 2.0, 3.0});
    }

    @Test
    public void streamFromCollections() {
        List<Integer> l = Arrays.asList(1, 2, 3);
        Stream<Integer> s = l.stream(); // any type of collection possible
        assertThat(s.toArray()).isEqualTo(new Integer[]{1, 2, 3});
    }

    @Test
    public void parallelStreamFromCollections() {
        Set<Integer> s = new HashSet<>(Arrays.asList(1, 2, 3));
        Stream<Integer> ps = s.parallelStream(); // any type of collection possible
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
    public void streamFromFile() throws IOException {
        try (Stream<String> s = Files.lines(Paths.get("C:\\sample.txt")))  {
            s.forEach(System.out::println);
        }
    }

    /**
     * Stream conversion
     */

    @Test
    public void listToString() {
        List<Integer> list = Arrays.asList(1, 2, 3, 5, 8, 13, 21);
        String result = list.stream().map(String::valueOf).reduce((a, b) -> a + ", " + b).orElseGet(String::new);
        assertThat(result).isEqualTo("1, 2, 3, 5, 8, 13, 21");
    }

    @Test
    public void listToArray() {
        List<Integer> list = Arrays.asList(1, 2, 3, 5, 8, 13, 21);
        // variant 1
        Integer[] result = list.stream().toArray(Integer[]::new);
        assertThat(result).isEqualTo(new Integer[]{1, 2, 3, 5, 8, 13, 21});
        // variant 2
        int[] result2 = list.stream().mapToInt(l -> l).toArray();
        assertThat(result2).isEqualTo(new Integer[]{1, 2, 3, 5, 8, 13, 21});
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

    @Test
    public void groupingBy() {
        List<Pair> pairs = Arrays.asList(new Pair(1, "A"), new Pair(1, "B"), new Pair(2, "C"), new Pair(3, "D"));
        Map<Integer, List<Pair>> groupedBy = pairs.stream().collect(Collectors.groupingBy(Pair::getId));
        assertThat(groupedBy.size()).isEqualTo(3);
        assertThat(groupedBy.get(1).size()).isEqualTo(2);
        assertThat(groupedBy.get(2).size()).isEqualTo(1);
        assertThat(groupedBy.get(3).size()).isEqualTo(1);
    }

    @Test
    public void sorted() {
        List<Integer> list = Arrays.asList(2, 3, 1);
        List<Integer> sorted = list.stream().sorted(Comparator.comparing(Integer::intValue)).collect(Collectors.toList());
        assertThat(sorted).isEqualTo(Arrays.asList(1, 2, 3));
    }

    @Test
    public void partition() {
        final int size = 2;
        final AtomicInteger counter = new AtomicInteger(0);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        Collection<List<Integer>> actual = list.stream().collect(Collectors.groupingBy(it -> counter.getAndIncrement() / size)).values();
        assertThat(actual.size()).isEqualTo(3);
        assertThat(new ArrayList<>(actual).get(0).size()).isEqualTo(2);
        assertThat(new ArrayList<>(actual).get(1).size()).isEqualTo(2);
        assertThat(new ArrayList<>(actual).get(2).size()).isEqualTo(1);
    }

    /**
     * Returns the number of the month ranging from 0..11.
     */
    private int month(Point point) {
        Instant instant = Instant.ofEpochMilli(point.timestamp);
        LocalDateTime dateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
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
