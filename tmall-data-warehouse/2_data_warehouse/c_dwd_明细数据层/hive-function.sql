
-- todo 函数 named_struct
DESC FUNCTION named_struct ;
/*
named_struct(name1, val1, name2, val2, ...)
    - Creates a struct with the given field names and values
*/

SELECT
    sku_id,
    -- 将多个字段组装为json字符串，指定key值
    named_struct(
        'attr_id', attr_id,
        'value_id', value_id,
        'attr_name', attr_name,
        'value_name', value_name
    )
FROM gmall.ods_sku_attr_value_full
WHERE dt='2024-09-11'
;

/*
1,"{""attr_id"":""106"",""value_id"":""176"",""attr_name"":""手机一级"",""value_name"":""安卓手机""}"
1,"{""attr_id"":""107"",""value_id"":""177"",""attr_name"":""二级手机"",""value_name"":""小米""}"
1,"{""attr_id"":""23"",""value_id"":""83"",""attr_name"":""运行内存"",""value_name"":""8G""}"
*/