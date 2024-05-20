import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

public class IMDBStudent20201003 {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: IMDBStudent20201003 <movies.dat> <ratings.dat> <k>");
            System.exit(-1);
        }

        String moviesPath = args[0];
        String ratingsPath = args[1];
        int k = Integer.parseInt(args[2]);

        // Spark 세션 생성
        SparkSession spark = SparkSession.builder().appName("TopKFantasyMovies").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // movies.dat 읽기
        JavaRDD<String> moviesLines = sc.textFile(moviesPath);

        // (movieId, title) 필터링하여 판타지 영화 추출
        JavaPairRDD<String, String> fantasyMovies = moviesLines
            .filter(line -> line.split("::")[2].contains("Fantasy"))
            .mapToPair(line -> {
                String[] parts = line.split("::");
                return new Tuple2<>(parts[0], parts[1]);
            });

        // ratings.dat 읽기
        JavaRDD<String> ratingsLines = sc.textFile(ratingsPath);

        // (movieId, rating) 추출
        JavaPairRDD<String, Tuple2<Integer, Integer>> movieRatings = ratingsLines
            .mapToPair(line -> {
                String[] parts = line.split("::");
                String movieId = parts[1];
                int rating = Integer.parseInt(parts[2]);
                return new Tuple2<>(movieId, new Tuple2<>(rating, 1));
            });

        // 영화 별로 평점 합계와 개수를 계산
        JavaPairRDD<String, Tuple2<Integer, Integer>> ratingTotalsAndCounts = movieRatings
            .reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()));

        // 영화 별 평균 평점을 계산
        JavaPairRDD<String, Double> averageRatings = ratingTotalsAndCounts
            .mapValues(totalAndCount -> (double) totalAndCount._1() / totalAndCount._2());

        // 판타지 영화의 평균 평점을 조인
        JavaPairRDD<String, Tuple2<String, Double>> fantasyMovieRatings = fantasyMovies
            .join(averageRatings);

        // 평균 평점이 높은 상위 k개의 영화 정렬
        List<Tuple2<String, Tuple2<String, Double>>> topKMovies = fantasyMovieRatings
            .mapToPair(tuple -> new Tuple2<>(tuple._2()._2(), tuple._2()._1()))
            .sortByKey(false)
            .take(k);

        // 결과 출력
        for (Tuple2<String, Double> movie : topKMovies) {
            System.out.println("(" + movie._2() + "," + movie._1() + ")");
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
