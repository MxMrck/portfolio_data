WITH gender_pixel AS (
  SELECT
  model_code_r3,
  CASE WHEN gender_id IN (4, 5, 6, 7, 8, 9) THEN 1
    ELSE 0 END is_junior,
  CASE WHEN gender_id IN (2, 5, 8, 14) THEN 1
    ELSE 0 END is_man,
  CASE WHEN gender_id IN (3, 6, 9, 15) THEN 1
    ELSE 0 END is_woman,
  CASE WHEN gender_id NOT IN (2, 3, 4, 5, 6, 7, 8, 9, 14, 15)
    OR gender_id IS NULL THEN 1 ELSE 0 END no_gender,
  MIN(gender_id) as gender_id,
  MIN(pixl_id) AS pixl_id
  FROM
    company_db.referentials.d_product_web_catalog
  WHERE
    locale = 'fr_CA'
    AND pixl_id IS NOT NULL
  GROUP BY
  1,2,3,4,5
),
base_ty AS (
  SELECT
  A.Year,
  CAST(A.timestamp AS DATE) AS date,
  CAL.date_of_day_comp AS comp_date,
  WEEKOFYEAR(DATE_ADD(A.timestamp, 1)) AS week_number,
  A.sales_support,
  A.item_operation_type,
  C.but_name_business_unit,
  case
  when A.transaction_touchpoint_name IN ('POS', 'SCO', 'MobilePOS') then 'physical'
  when
  A.transaction_touchpoint_name in ('Desktop', 'WebMobile', 'MobileApplication', 'Decashop')
  AND A.transaction_touchpoint_type = 'digital'
  then
  'Web'
  when A.transaction_touchpoint_name = 'MobileApplication' then 'DecathApp'
  when A.transaction_touchpoint_name = 'TeamMem' then 'AssistedInStore'
  else 'Unknown'
  end as transaction_touchpoint,
  coalesce(B.unv_label, 'NA') as unv_label,
  coalesce(B.category_label, 'NA') as category_label,
  coalesce(B.dpt_label, 'NA') as sport,
  coalesce(B.brd_label_brand, 'NA') as label_brand,
  coalesce(B.brd_num_brand, 9999) As num_brand,
  coalesce(B.brd_type_brand_libelle, 'Unknown') As type_brand,
  coalesce(B.family_label, 'NA') as family_label,
  coalesce(B.fam_num_family, 9999) as family_id,
  coalesce(B.product_nature_label, 'NA') as nature,
  coalesce(B.mdl_label, 'NA') as mdl_label,
  coalesce(B.mdl_num_model_r3, 9999) as mdl_num,
  coalesce(B.grid_size, 'NA') as size,
  'https://contents.adress.com/p'
  || COALESCE(e.pixl_id, '0000')
  || '/sq/'
  || COALESCE(e.pixl_id, '0000')
  || '.jpg?f=224x22' AS image_url,
  coalesce(e.is_junior, 9999) as is_junior,
  coalesce(e.is_man, 9999) as is_man,
  coalesce(e.is_woman, 9999) as is_woman,
  coalesce(e.no_gender, 9999) as no_gender,
  COUNT(A.transaction_id) AS nb_transactions_ty,
  SUM(A.sales) As to_ty,
  SUM(A.gmv_amount) AS gmv_ty,
  SUM(A.margin) As margin_ty,
  SUM(A.gmv_item_quantity) As qt_ty
  FROM
  company_db.sales.sales_detail AS A
  LEFT JOIN analytics_db.datamart.calendar AS CAL
    ON date(A.timestamp) = CAL.date_of_day
  LEFT JOIN company_db.referential.d_sku AS B
    ON A.item_code = B.sku_num_sku_r3
  AND B.sku_date_end > current_date
  LEFT JOIN company_db.referentials.d_business_unit AS C
    ON A.businessunit_gln = C.but_code_international
  LEFT JOIN gender_pixel e
    ON B.mdl_num_model_r3 = e.model_code_r3
  WHERE
    A.businessunit_country_code = 'CA'
    AND A.timestamp >= date_add(current_date, -3 * 365 - 7)
  GROUP BY ALL
),
dims AS (
select distinct
sales_support,
item_operation_type,
but_name_business_unit,
transaction_touchpoint,
unv_label,
category_label,
sport,
label_brand, --new
num_brand, --new
type_brand, -- new
family_label,
family_id,
nature,
mdl_label,
mdl_num,
image_url, 
size,
is_junior,
is_man,
is_woman,
no_gender
from
base_ty
),
all_t as (
Select
substr(D.week_YYYYWW, 1, 4) as year,
substr(D.week_YYYYWW, 7, 2) as week_number,
D.date_of_day as date,
D.date_of_day_comp as comp_date,
A.*,
--B.image_url,
B.to_ty,
B.gmv_ty,
B.margin_ty,
B.qt_ty,
B.nb_transactions_ty,
C.to_ty AS to_ly,
C.gmv_ty AS gmv_ly,
C.margin_ty AS margin_ly,
C.qt_ty AS qt_ly,
C.nb_transactions_ty AS nb_transactions_ly
from
analytics_db.datamart.dim_calendar as D
inner join dims as A
on 1 = 1
left join base_ty as B
ON D.date_of_day = B.date
AND A.unv_label = B.unv_label
AND A.but_name_business_unit = B.but_name_business_unit
AND A.transaction_touchpoint = B.transaction_touchpoint
AND A.item_operation_type = B.item_operation_type
AND A.sales_support = B.sales_support
AND A.category_label = B.category_label
AND A.sport = B.sport
AND A.label_brand = B.label_brand --new
AND A.num_brand = B.num_brand --new
AND A.type_brand = B.type_brand --new
AND A.family_label = B.family_label
AND A.family_id = B.family_id
AND A.nature = B.nature
AND A.mdl_label = B.mdl_label
AND A.mdl_num = B.mdl_num
AND A.size = B.size 
AND A.image_url = B.image_url 
AND A.is_junior = B.is_junior
AND A.is_man = B.is_man
AND A.is_woman = B.is_woman
AND A.no_gender = B.no_gender
left join base_ty as C
ON D.date_of_day_comp = C.date
AND A.unv_label = C.unv_label
AND A.but_name_business_unit = C.but_name_business_unit
AND A.transaction_touchpoint = C.transaction_touchpoint
AND A.item_operation_type = C.item_operation_type
AND A.sales_support = C.sales_support
AND A.category_label = C.category_label
AND A.sport = C.sport
AND A.label_brand = C.label_brand --new
AND A.num_brand = C.num_brand --new
AND A.type_brand = C.type_brand --new
AND A.family_label = C.family_label
AND A.family_id = C.family_id
AND A.nature = C.nature
AND A.mdl_label = C.mdl_label
AND A.mdl_num = C.mdl_num
AND A.size = C.size -- new
AND A.image_url = C.image_url -- NEW
AND A.is_junior = C.is_junior
AND A.is_man = C.is_man
AND A.is_woman = C.is_woman
AND A.no_gender = C.no_gender
where
D.date_of_day between date_add(current_date, -3 * 365 - 7) and current_date
),
df AS (
SELECT
date,
week_number,
sales_support,
item_operation_type,
but_name_business_unit,
transaction_touchpoint,
unv_label,
category_label,
sport,
label_brand, --new
num_brand, --new
type_brand, --new
family_label,
family_id,
nature,
mdl_label,
mdl_num,
size,
image_url,
is_junior,
is_man,
is_woman,
no_gender,
SUM(
CASE WHEN sales_support = 'InStore' THEN coalesce(to_ty) else 0
end) AS in_to_ty,
SUM(CASE WHEN sales_support = 'OutStore' THEN coalesce(to_ty) else 0
end) AS out_to_ty,
SUM(CASEWHEN sales_support = 'InStore' THEN coalesce(to_ly) else 0
end) AS in_to_ly,
SUM(CASEWHEN sales_support = 'OutStore' THEN coalesce(to_ly) else 0
end) AS out_to_ly,
SUM(CASE WHEN sales_support = 'InStore' THEN coalesce(gmv_ty) else 0
end) AS in_gmv_ty,
SUM(CASE WHEN sales_support = 'OutStore' THEN coalesce(gmv_ty) else 0
end) AS out_gmv_ty,
SUM(CASEWHEN sales_support = 'InStore' THEN coalesce(gmv_ly) else 0
end) AS in_gmv_ly,
SUM(CASE WHEN sales_support = 'OutStore' THEN coalesce(gmv_ly) else 0
end) AS out_gmv_ly,
SUM(CASE WHEN sales_support = 'InStore' THEN coalesce(margin_ty)else 0
end) AS in_margin_ty,
SUM(CASE WHEN sales_support = 'InStore' THEN coalesce(margin_ly) else 0
end) AS in_margin_ly,
SUM(CASE WHEN sales_support = 'OutStore' THEN coalesce(margin_ty) else 0
end) AS out_margin_ty,
SUM(CASE WHEN sales_support = 'OutStore' THEN coalesce(margin_ly) else 0
end) AS out_margin_ly,
SUM(CASE WHEN sales_support = 'InStore' THEN coalesce(qt_ty) else 0
end) AS in_qt_ty,
SUM(CASE WHEN sales_support = 'InStore' THEN coalesce(qt_ly) else 0
end) AS in_qt_ly,
SUM(CASE WHEN sales_support = 'OutStore' THEN coalesce(qt_ty)else 0
end) AS out_qt_ty,
SUM(CASE WHEN sales_support = 'OutStore' THEN coalesce(qt_ly) else 0
end) AS out_qt_ly,
SUM(CASE WHEN sales_support = 'InStore' THEN coalesce(nb_transactions_ty) else 0
end) AS in_nb_transactions_ty,
SUM(CASE WHEN sales_support = 'InStore' THEN coalesce(nb_transactions_ly) else 0
end) AS in_nb_transactions_ly,
SUM(CASE WHEN sales_support = 'OutStore' THEN coalesce(nb_transactions_ty) else 0
end) AS out_nb_transactions_ty,
SUM(CASE WHEN sales_support = 'OutStore' THEN coalesce(nb_transactions_ly) else 0
end) AS out_nb_transactions_ly
FROM
  all_t
WHERE
  1 = 1
GROUP BY ALL
)
SELECT
  *
FROM
  df
WHERE
  COALESCE(in_to_ly, 0) != 0
  OR COALESCE(in_to_ty, 0) != 0
  OR COALESCE(out_to_ly, 0) != 0
  OR COALESCE(out_to_ty, 0) != 0
