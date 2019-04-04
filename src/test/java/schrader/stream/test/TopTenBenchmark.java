import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

public class TopTenBenchmark {

    private Stream<String> fileLines(String path) {
        try {
            return Files.lines(Paths.get(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BenchmarkMode(Mode.SampleTime)
    @Benchmark
    public void topten(Blackhole blackhole) {
        Arrays.stream(new String[]{"large.txt"})
                .flatMap(this::fileLines)
                .flatMap(line -> Arrays.stream(line.split("\\b")));
    }
}
