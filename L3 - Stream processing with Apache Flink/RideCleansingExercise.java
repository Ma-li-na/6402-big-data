package com.ververica.flinktraining.exercises.datastream_java.basics;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RideCleansingExercise extends ExerciseBase 
{
    public static void main(String[] args) throws Exception 
    {

        // Получаем параметры из командной строки
        ParameterTool params = ParameterTool.fromArgs(args);
        // Получаем путь к файлу с данными о поездках, если параметр "input" не указан, используем путь по умолчанию
        final String input = params.get("input", ExerciseBase.pathToRideData);
        System.out.println("point 0");
        // Максимальная задержка события (в секундах)
        final int maxEventDelay = 60;      
        // Фактор скорости обработки событий (события за 10 минут обрабатываются за 1 секунду)
        final int servingSpeedFactor = 600; 
        System.out.println("point 1");
        // Настраиваем окружение для потоковой обработки данных
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);
        System.out.println("point 2");
        // Создаем источник данных о поездках 
        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));
        System.out.println("point 3");
        // Фильтруем поездки, оставляя только те, которые начинаются и заканчиваются в Нью-Йорке
        DataStream<TaxiRide> filteredRides = rides
                .filter(new NYCFilter());
        System.out.println("point 4");
        // Выводим отфильтрованный поток данных
        printOrTest(filteredRides);
        env.execute("Taxi Ride Cleansing");
    }

    // Класс фильтра для отбора поездок, начинающихся и заканчивающихся в Нью-Йорке
    public static class NYCFilter implements FilterFunction<TaxiRide> 
    {
        // Метод фильтрации, возвращает true, если поездка начинается и заканчивается в Нью-Йорке
        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception 
        {
            // Проверяем, находятся ли начальные и конечные координаты поездки в пределах Нью-Йорка
            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }
}