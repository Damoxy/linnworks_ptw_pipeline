INSERT INTO [lw].[Order_sales] (
    OrderDate,
    DispatchDate
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
    fs.DispatchDate AS DispatchDate,

    -- ItemID: use Product_Code instead of ProductKey
    dp.Product_Code AS ItemID,

    -- UnitCost = ABS(Total Cost) / Quantity
    CASE WHEN fs.Quantity <> 0 
         THEN ABS(fs.[Total Cost]) / fs.Quantity 
         ELSE 0 END AS UnitCost,

    -- TotalCost = ABS(Total Cost)
    ABS(fs.[Total Cost]) AS TotalCost,

    -- UnitPrice = Base Line Value / Quantity
    CASE WHEN fs.Quantity <> 0 
         THEN fs.[Base Line Value] / fs.Quantity 
         ELSE 0 END AS UnitPrice,

    -- TotalPrice = Base Line Value
    fs.[Base Line Value] AS TotalPrice,

    -- UnitTax = Base Line VAT Value / Quantity
    CASE WHEN fs.Quantity <> 0 
         THEN fs.[Base Line VAT Value] / fs.Quantity 
         ELSE 0 END AS UnitTax,

    -- TotalTax = Base Line VAT Value
    fs.[Base Line VAT Value] AS TotalTax,

    -- TotalIncTax = Base Gross Line Value
    fs.[Base Gross Line Value] AS TotalIncTax,

    -- Quantity
    fs.Quantity,

    -- SubItemSKU = N/A
    NULL AS SubItemSKU,

    -- SubItemUnitCost = N/A
    NULL AS SubItemUnitCost,

    -- SubItemQty = N/A
    NULL AS SubItemQty,

    -- fkOrderId
    fs.Sales_Order_No AS fkOrderId,

    -- FkLocationId
    fs.WarehouseKey AS FkLocationId,

    -- ItemSource = N/A
    NULL AS ItemSource,

    -- fkStockId = N/A
    NULL AS fkStockItemId,

    -- Source
    'maginus' AS Source
FROM [MaginusOMS].[dbo].[fct_Sales] fs
INNER JOIN [MaginusOMS].[dbo].[Dim_Product] dp
    ON fs.ProductKey = dp.ProductKey;


