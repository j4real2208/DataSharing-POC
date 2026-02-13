docker network create --subnet=172.30.0.0/16 minikube-shared

psql -U postgres -d source_db -h localhost


CREATE TABLE IF NOT EXISTS public.weather_readings (
 id serial PRIMARY KEY,
 city text NOT NULL,
 temperature_c numeric(5,2) NOT NULL,
 observed_at timestamptz NOT NULL DEFAULT now()
);

 INSERT INTO public.weather_readings (city, temperature_c) VALUES ('Testville', 12.3);
 INSERT INTO public.weather_readings (city, temperature_c) VALUES ('Testville1', 11.3);
 INSERT INTO public.weather_readings (city, temperature_c) VALUES ('Testville2', 15.3);


INSERT INTO public.weather_readings (city, temperature_c) VALUES
  ('Seattle', 14.3),
  ('Austin', 27.8),
  ('Chicago', 9.1);


 SELECT * FROM public.weather_readings;



 bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
   --topic source.public.weather_readings --from-beginning --max-messages 1


   bin/kafka-topics.sh --list --bootstrap-server localhost:9092


    kubectl --context minikube-a -n messaging run -it --rm netcheck \
     --image=busybox:1.36 --restart=Never



     psql -U postgres -d sink_db -h localhost
