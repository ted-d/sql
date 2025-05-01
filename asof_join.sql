/*CLICKHOUSE


Давайте оценим как менялись глобальные продажи от года к году для приставок PS3, PS2, X360, Wii. 

Для этого нужно выполнить следующие шаги. Посчитать продажи за каждый год по каждой платформе, отфильтровать строки с пустыми значениями в колонке Year.
Затем нужно продублировать данный запрос и объединить два одинаковых запроса друг с другом так, чтобы данные за предыдущий год были в текущем.

У вас могут возникнуть проблемы с датой, воспользуйтесь функцией, которую мы изучили ранее, parseDateTimeBestEffort.

В задаче нужно использовать ASOF JOIN для объединения данных. Также у вас может возникнуть сложность при объединении, если вы воспользуетесь синтаксисом using(id, dt).
В данной задаче нужно использовать ASOF JOIN с таким синтаксисом: ON ... = ... AND ... > или < .... Последнее условие указывает, по какому принципу объединять данные, которые не совпадают.

CREATE TABLE video_game_sales (
    Rank UInt32,
    Name String,
    Platform String,
    Year String,
    Genre String,
    Publisher String,
    NA_Sales Float32,
    EU_Sales Float32,
    JP_Sales Float32,
    Other_Sales Float32,
    Global_Sales Float32
) ENGINE = Log

INSERT INTO video_game_sales SELECT * FROM url('https://raw.githubusercontent.com/dmitrii12334/clickhouse/main/vgsale', CSVWithNames, 'Rank UInt32,
    Name String,
    Platform String,
    Year String,
    Genre String,
    Publisher String,
    NA_Sales Float32,
    EU_Sales Float32,
    JP_Sales Float32,
    Other_Sales Float32,
    Global_Sales Float32');

Формат ответа:

После чего просто возьмите сумму от столбца разницы текущего и предыдущего года. У вас получится отрицательное число, впишите ответ по модулю округленный до целого числа.
Важно, отфильтруйте все строки где предыдущий год равен 0

Тестовая таблица:

В курсе вы встретите такие примеры, это так называемые VIEW по факту это запрос с именем, для вашей проверки, к нему можно обращаться как к обычной таблице, в данном случае вот так select ...
from ideo_game_sales_view Уточню что на данной витрине вы тестируете свои запросы для проверки полученного ответа. Ответ нужно вводить по всей выборке!

create view video_game_sales_view as select * from video_game_sales limit 200

Вывод для правильного запроса (вывод таблицы только для PS2, сумма для всех платформ):

| Platform | Year | f2.Year | cur_sale            | prev_sale           | diff                 |
|----------|------|---------|---------------------|---------------------|----------------------|
| PS2      | 2001 | 2000    | 3.509999990463257   | 0.2199999988079071  | 3.2899999916553497   |
| PS2      | 2002 | 2001    | 1.8500000089406967  | 3.509999990463257   | -1.6599999815225601  |
| PS2      | 2003 | 2002    | 0.2199999988079071  | 1.8500000089406967  | -1.6300000101327896  |
| PS2      | 2004 | 2003    | 1.979999989271164   | 0.2199999988079071  | 1.7599999904632568   |
| PS2      | 2005 | 2004    | 0.7699999958276749  | 1.979999989271164   | -1.209999993443489   |
| PS2      | 2006 | 2005    | 0.32999999821186066 | 0.7699999958276749  | -0.4399999976158142  |
| PS2      | 2007 | 2006    | 0.2199999988079071  | 0.32999999821186066 | -0.10999999940395355 |
| PS2      | 2008 | 2007    | 1.969999998807907   | 0.2199999988079071  | 1.75                 |
| PS2      | 2009 | 2008    | 0.10999999940395355 | 1.969999998807907   | -1.8599999994039536  |
| PS2      | 2011 | 2009    | 0.10999999940395355 | 0.10999999940395355 | 0                    |

-89.9399


*/
WITH 
t1 AS (
    SELECT 
        parseDateTimeBestEffort(Year) AS year,
        Platform,
        sum(Global_Sales) AS GS 
    FROM video_game_sales
    WHERE Year != '' --нужно убирать пустые значения , так как ASOF не работает с NULL значениями!
      AND Platform IN ('PS3', 'PS2', 'X360', 'Wii')
    GROUP BY parseDateTimeBestEffort(Year), Platform
),
t2 AS (
    SELECT 
        parseDateTimeBestEffort(Year) AS year,
        Platform,
        sum(Global_Sales) AS GS 
    FROM video_game_sales
    WHERE Year != '' 
      AND Platform IN ('PS3', 'PS2', 'X360', 'Wii')
    GROUP BY parseDateTimeBestEffort(Year), Platform
),
t3 as (SELECT 
    t1.Platform,
    t1.year AS year1,
    t2.year AS year2,
    t1.GS AS GS1,
    t2.GS AS GS2,
    t1.GS - t2.GS AS GS_diff
FROM t1 
ASOF JOIN t2 ON t1.Platform = t2.Platform and t1.year>t2.year
ORDER BY t2.year, t1.year)
select abs(round(sum(GS_diff))) from t3;
