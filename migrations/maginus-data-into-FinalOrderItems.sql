INSERT INTO [linnworks].[lw].[final_orderitems] (
    final_sku,
    final_quantity,
    final_price,
    final_cost,
    TotalFinalPrice,
    TotalFinalCost,
    final_date,
    kitsku,
    source
)
SELECT
    PRODUCT_CODE AS final_sku,
    ACTUAL_QUANTITY AS final_quantity,
    UNIT_PRICE AS final_price,
    UNIT_COST AS final_cost,
    (UNIT_PRICE * ACTUAL_QUANTITY) AS TotalFinalPrice,
    (UNIT_COST  * ACTUAL_QUANTITY) AS TotalFinalCost,
    DELIVERY_DATE AS final_date,
    NULL AS kitsku,
    'maginus' AS source
FROM [MaginusOMS].[dbo].[PICK_DESPATCH_ITEM];
