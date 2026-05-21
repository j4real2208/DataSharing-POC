psql -U postgres -d source_db -h localhost

INSERT INTO public.weather_readings (city, temperature_c) VALUES ('Testville', 12.3);
INSERT INTO public.weather_readings (city, temperature_c) VALUES ('Testville1', 11.3);
INSERT INTO public.weather_readings (city, temperature_c) VALUES ('Testville2', 15.3);


INSERT INTO public.weather_readings (city, temperature_c) VALUES
('Seattle', 14.3),
('Austin', 27.8),
('Chicago', 9.1);


SELECT * FROM public.weather_readings;

