INSERT INTO [lw].[Order_sales] (
    OrderDate,
    DispatchDate,
    ItemID,
    UnitCost,
    TotalCost,
    UnitPrice,
    TotalPrice,
    UnitTax,
    TotalTax,
    TotalIncTax,
    Quantity,
    SubItemSKU,
    SubItemUnitCost,
    SubItemQty,
    fkOrderId,
    FkLocationId,
    ItemSource,
    fkStockItemId,
    Source
)
SELECT
    -- OrderDate (convert DateKey to datetime)
    CONVERT(DATETIME,
            STUFF(STUFF(CAST(fs.DateKey AS CHAR(8)), 5, 0, '/'), 8, 0, '/') + ' 00:00:00') AS OrderDate,
     
    -- Order DispatchDate
    fs.Despatch_Date AS DispatchDate,
