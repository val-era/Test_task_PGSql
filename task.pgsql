WITH new_credits AS (
SELECT 
DENSE_RANK() OVER(PARTITION BY deal ORDER BY deal, date) AS rak,
date,
deal,
sum,
SUM(sum) OVER(PARTITION BY deal ORDER BY deal, date) AS Total
FROM credits
)


SELECT * FROM new_credits ;

SELECT * FROM new_credits WHERE rak <= (SELECT MAX(rak) FROM new_credits WHERE total <=0) ;

--Создаем функцию в результате которой выходит табличка в которой отображаются--
--результаты от начала отсчета до первого значения выплаченной суммы <=0--
CREATE OR REPLACE FUNCTION some_functio() returns table(rak bigint, date date, deal bigint, sum integer, total bigint) AS
$$

declare
	f record;
	--Рекорд делал для отображения промежуточных результатов в записи, --
	--но забыл убрать, и не буду что-бы работало) Отображение через RAISE NOTICE (% %) и значения вместо %--
begin
	-- Пробегаемся циклом по табличке где через оконную добавляется сумма пошаговая прошлой суммы--
	-- с текущей выплатой. и выставляется ранк по дате, для удобства вычисления--
	-- далее вычисляется ранг где сумма <=0 и идет возвращение таблички где ранк <= ранку--
	-- с суммой 0 и меньше и с номерами кредитов, для того что-бы удалять кредиты не просто по-- 
	-- рангу но и по номеру--
	for f in select def.rak, def.deal  from 
	(SELECT * FROM(
		select * from(
			SELECT 
			DENSE_RANK() OVER(PARTITION BY credits.deal ORDER BY credits.deal, credits.date) AS rak,
			credits.date, credits.deal, credits.sum,
			SUM(credits.sum) OVER(PARTITION BY credits.deal ORDER BY credits.deal, credits.date) AS Total
			FROM credits)def_two order by def_two.deal, def_two.date )foo
			WHERE foo.total <= 0 order by foo.deal, foo.date) AS def
	loop
	RETURN QUERY SELECT * from (SELECT 
			DENSE_RANK() OVER(PARTITION BY credits.deal ORDER BY credits.deal, credits.date) AS rak,
			credits.date, credits.deal,  credits.sum,
			SUM(credits.sum) OVER(PARTITION BY credits.deal ORDER BY credits.deal, credits.date) AS Total
			FROM credits )d where d.deal = f.deal AND d.rak <= f.rak;
	end loop;
end;
$$ LANGUAGE plpgsql VOLATILE;

-- Через WITH создаю несколько буфферных таблиц, в первую вношу рез-т функции--
--Во вторую вношу основную табличку со всеми данными с колонками ранка и суммы, которые из-за одинаковой сортировки--
-- одинаковы. И в 3 табличке, я нахожу уникальные значения, между 2мя табличками т.е значения больше 0--
WITH new_credits AS (
	SELECT DISTINCT  * FROM some_functio() ORDER BY deal, date
), finally_credits AS(
	SELECT DENSE_RANK() OVER(PARTITION BY deal ORDER BY deal, date) AS rak,
	date, deal, sum,
	SUM(sum) OVER(PARTITION BY deal ORDER BY deal, date) AS Total
	FROM credits
), result_table AS(
	SELECT * FROM finally_credits EXCEPT 
	SELECT * FROM new_credits ORDER BY deal, date
)

-- Тут делаю табличку на выход, где во второй колонке кладу минимальную дату, по каждому номеру--
-- Что бы выяснить минимальную дату просрочки--
-- В 3 колонку кладу последнее значение суммы по номеру, которая показывает сумму к оплате--
-- Ну и как фишка сделал разницу в днях с текущего дня до даты просроченного платежа--
SELECT DISTINCT deal AS deal_number,
MIN(date) OVER(PARTITION BY deal) AS Date_of_overdue_payment,
LAST_VALUE(total) OVER(PARTITION BY deal) AS summ_to_pay,
'TODAY'::timestamp - MIN(date) OVER(PARTITION BY deal) AS Difference_of_days 
FROM result_table ORDER BY deal;
