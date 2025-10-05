INSERT INTO lw.StockLevel (
    fkStockItemId,
    fkStockLocationId,
    Quantity,
    OnOrder,
    CurrentStockValue,
    MinimumLevel,
    AutoAdjust,
    LastUpdateDate,
    LastUpdateOperation,
    rowid,
    PendingUpdate,
    InOrderBook,
    JIT
)
SELECT
    j.StockItemId,
    j.StockLocationId,
    j.StockLevel,
    j.InOrders,
    j.StockValue,
    j.MinimumLevel,
    j.AutoAdjust,
    j.LastUpdateDate,
    j.LastUpdateOperation,
    j.rowid,
    j.PendingUpdate,
    j.InOrderBook,
    j.JIT
FROM [linnworks].[staging].[_airbyte_raw_stock_items] d
CROSS APPLY OPENJSON(d.StockLevels) 
WITH (
    StockItemId UNIQUEIDENTIFIER '$.StockItemId',
    StockLocationId UNIQUEIDENTIFIER '$.Location.StockLocationId',
    StockLevel INT '$.StockLevel',
    InOrders INT '$.InOrders',
    StockValue FLOAT '$.StockValue',
    MinimumLevel INT '$.MinimumLevel',
    AutoAdjust BIT '$.AutoAdjust',
    LastUpdateDate DATETIME '$.LastUpdateDate',
    LastUpdateOperation VARCHAR(64) '$.LastUpdateOperation',
    rowid UNIQUEIDENTIFIER '$.rowid',
    PendingUpdate BIT '$.PendingUpdate',
    InOrderBook INT '$.InOrderBook',
    JIT BIT '$.JIT'
) AS j