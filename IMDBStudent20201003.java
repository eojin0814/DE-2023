import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IMDBStudent20201003 {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: IMDBStudent20201003 <input path> <intermediate output path> <k>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String intermediateOutputPath = args[1];
        int k = Integer.parseInt(args[2]);

        // Spark 세션 생성
        SparkSession spark = SparkSession.builder().appName("TopKMovies").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // 입력 파일 읽기
        JavaRDD<String> lines = sc.textFile(inputPath);

        // (영화ID, (평점, 1)) 형식의 PairRDD 생성
        JavaPairRDD<String, Tuple2<Integer, Integer>> movieRatings = lines.mapToPair(line -> {
            String[] attributes = line.split("::");
            String movieId = attributes[1];
            int rating = Integer.parseInt(attributes[2]);
            return new Tuple2<>(movieId, new Tuple2<>(rating, 1));
        });

        // 각 영화의 평점 합계와 개수를 계산
        JavaPairRDD<String, Tuple2<Integer, Integer>> ratingTotalsAndCounts = movieRatings.reduceByKey(
            (a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2())
        );

        // 각 영화의 평균 평점을 계산
        JavaPairRDD<String, Double> averageRatings = ratingTotalsAndCounts.mapValues(
            totalAndCount -> (double) totalAndCount._1() / totalAndCount._2()
        );

        // 중간 결과 저장 (선택 사항)
        averageRatings.saveAsTextFile(intermediateOutputPath);

        // 평균 평점을 기준으로 상위 k개의 영화를 정렬
        List<Tuple2<String, Double>> topKMovies = averageRatings
            .top(k, new Tuple2Comparator());

        // 결과 출력
        for (Tuple2<String, Double> movie : topKMovies) {
            System.out.println(movie._1() + " : " + movie._2());
        }

        // Spark 세션 종료
        spark.stop();
    }

    static class Tuple2Comparator implements java.util.Comparator<Tuple2<String, Double>>, java.io.Serializable {
        @Override
        public int compare(Tuple2<String, Double> t1, Tuple2<String, Double> t2) {
            return t2._2().compareTo(t1._2());
        }
    }
}
