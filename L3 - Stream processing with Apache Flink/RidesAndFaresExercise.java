package com.ververica.flinktraining.exercises.datastream_java.state;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;


public class RidesAndFaresExercise extends ExerciseBase
{
  public static void main(String[] args) throws Exception
  {
    // Получаем параметры из командной строки
    ParameterTool params = ParameterTool.fromArgs(args);
    // Получаем путь к файлу с данными о поездках
    final String ridesFile = params.get("rides", pathToRideData);
    // Получаем путь к файлу с данными об оплатах
    final String faresFile = params.get("fares", pathToFareData);
    // Максимальная задержка событий в секундах
    final int delay = 60;    
    // Фактор скорости обработки событий (например, 30 минут событий обрабатываются за 1 секунду)
    final int servingSpeedFactor = 1800;  

    // Настраиваем окружение потоковой обработки, включая веб-интерфейс и REST endpoint
    Configuration conf = new Configuration();
    conf.setString("state.backend", "filesystem"); // Указываем файловую систему как backend для хранения состояния
    conf.setString("state.savepoints.dir", "file:///tmp/savepoints"); // Указываем директорию для сохранения savepoints
    conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints"); // Указываем директорию для сохранения checkpoints
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf); // Создаем локальное окружение с веб-интерфейсом
    env.setParallelism(ExerciseBase.parallelism); 
    env.enableCheckpointing(10000L); // Включаем checkpointing с интервалом 10 секунд
    CheckpointConfig config = env.getCheckpointConfig();
    config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); 

    // Создаем поток данных о поездках
    DataStream<TaxiRide> rides = env
        .addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, delay, servingSpeedFactor))) // Добавляем источник данных о поездках
        .filter((TaxiRide ride) -> ride.isStart) // Фильтруем только события начала поездки
        .keyBy(ride -> ride.rideId); 

    // Создаем поток данных об оплатах
    DataStream<TaxiFare> fares = env
        .addSource(fareSourceOrTest(new TaxiFareSource(faresFile, delay, servingSpeedFactor))) // Добавляем источник данных об оплатах
        .keyBy(fare -> fare.rideId); 

    // Устанавливаем UID для stateful flatmap operator, чтобы можно было читать его состояние с помощью State Processor API.
    DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
        .connect(fares) // Соединяем потоки поездок и оплат
        .flatMap(new EnrichmentFunction()) 
        .uid("enrichment"); 

    printOrTest(enrichedRides); 
    env.execute("Join Rides with Fares (java RichCoFlatMap)"); 
  }

  public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>>
  {
    private ValueState<TaxiRide> rideState; // Состояние для хранения TaxiRide
    private ValueState<TaxiFare> fareState; // Состояние для хранения TaxiFare

    @Override
    public void open(Configuration config)
    {
      rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class)); // Создаем дескриптор состояния для TaxiRide
      fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class)); // Создаем дескриптор состояния для TaxiFare
    }

    // Обрабатываем TaxiRide
    @Override
    public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception
    {
      TaxiFare fare = fareState.value(); // Получаем сохраненную оплату из состояния
      if (fare != null)
      {
        fareState.clear(); // Очищаем состояние оплаты
        out.collect(new Tuple2(ride, fare)); // Отправляем объединенные данные
      } else
      {
        rideState.update(ride); // Сохраняем поездку в состояние
      }
    }

    // Обрабатываем TaxiFare
    @Override
    public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception
    {
      TaxiRide ride = rideState.value(); // Получаем сохраненную поездку 
      if (ride != null)
      {
        rideState.clear(); // Очищаем состояние поездки
        out.collect(new Tuple2(ride, fare)); // Отправляем объединенные данные
      } else
      {
        fareState.update(fare); // Сохраняем оплату в состояние
      }
    }
  }
}
