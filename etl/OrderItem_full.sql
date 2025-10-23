WITH ParentItems AS (
    SELECT
        parent.OrderId,
        parent.ItemId AS ParentItemId,
        parent.Title AS ParentTitle,
        parent.SKU AS ParentSKU,
        parent.Quantity AS ParentQty,
        parent.UnitCost AS ParentUnitCost,
        parent.PricePerUnit - (parent.DiscountValue / NULLIF(parent.Quantity, 0)) AS PricePerUnit,
        parent.Tax AS ParentTax,
        parent.TaxRate AS ParentTaxRate,
        parent.CostIncTax AS ParentTotalIncTax,
        parent.Weight AS ParentWeight,
        br.BinRack AS ParentBinRack,
        br.Location AS LocationId,
        parent.CompositeSubItems,
        parent.ItemSource AS ParentItemSource,
        parent.StockItemId AS ParentStockItemId
    FROM [linnworks].[staging].[_airbyte_raw_processed_order_details] t
    CROSS APPLY OPENJSON(t.Items)
        WITH (
            OrderId UNIQUEIDENTIFIER,
            ItemId UNIQUEIDENTIFIER,
            Title NVARCHAR(255),
            SKU NVARCHAR(50),
            Quantity INT,
            UnitCost DECIMAL(18,4),
            PricePerUnit DECIMAL(18,4),
            DiscountValue DECIMAL(18,4),
            Tax DECIMAL(18,4),
            TaxRate DECIMAL(5,2),
            CostIncTax DECIMAL(18,4),
            Weight DECIMAL(18,4),
            BinRacks NVARCHAR(MAX) AS JSON,
            CompositeSubItems NVARCHAR(MAX) AS JSON,
            ItemSource NVARCHAR(50),
            StockItemId UNIQUEIDENTIFIER
        ) AS parent
    OUTER APPLY OPENJSON(parent.BinRacks)
        WITH (
            BinRack NVARCHAR(50),
            Location NVARCHAR(100)
        ) AS br
),
SubItems AS (
    SELECT
        p.OrderId,
        p.ParentItemId,
        p.ParentTitle,
        p.ParentSKU,
        p.ParentQty,
        p.ParentUnitCost,
        p.PricePerUnit AS ParentSellPrice,
        p.ParentTax,
        p.ParentTaxRate,
        p.ParentTotalIncTax,
        p.ParentWeight,
        p.ParentBinRack,
        p.LocationId,
        p.ParentItemSource,
        p.ParentStockItemId,
        sub.ItemId,
        sub.Title,
        sub.SKU,
        sub.Quantity,
        sub.UnitCost,
        sub.PricePerUnit - (sub.DiscountValue / NULLIF(sub.Quantity, 0)) AS PricePerUnit,
        sub.Tax,
        sub.TaxRate,
        sub.CostIncTax,
        sub.Weight,
        sbr.BinRack,
        sub.ItemSource,
        sub.StockItemId
    FROM ParentItems p
    OUTER APPLY OPENJSON(p.CompositeSubItems)
        WITH (
            OrderId UNIQUEIDENTIFIER,
            ItemId UNIQUEIDENTIFIER,
            Title NVARCHAR(255),
            SKU NVARCHAR(50),
            Quantity INT,
            UnitCost DECIMAL(18,4),
            PricePerUnit DECIMAL(18,4),
            DiscountValue DECIMAL(18,4),
            Tax DECIMAL(18,4),
            TaxRate DECIMAL(5,2),
            CostIncTax DECIMAL(18,4),
            Weight DECIMAL(18,4),
            BinRacks NVARCHAR(MAX) AS JSON,
            ItemSource NVARCHAR(50),
            StockItemId UNIQUEIDENTIFIER
        ) AS sub
    OUTER APPLY OPENJSON(sub.BinRacks)
        WITH (
            BinRack NVARCHAR(50),
            Location NVARCHAR(100)
        ) AS sbr
)
INSERT INTO [linnworks].[lw].[OrderItem_full] (
    OrderId,
    ParentItemId,
    ParentTitle,
    ParentSKU,
    ParentQty,
    ParentUnitCost,
    ParentSellPrice,
    ParentTax,
    ParentTaxRate,
    ParentTotalIncTax,
    ParentWeight,
    ParentBinRack,
    LocationId,
    ParentItemSource,
    ParentStockItemId,
    SubItemId,
    SubItemTitle,
    SubItemSKU,
    SubItemQty,
    SubItemUnitCost,
    SubItemSellPrice,
    SubItemTax,
    SubItemTaxRate,
    SubItemTotalIncTax,
    SubItemWeight,
    SubItemBinRack,
    SubItemItemSource,
    SubItemStockItemId
)
SELECT
    OrderId,
    ParentItemId,
    ParentTitle,
    ParentSKU,
    ParentQty,
    ParentUnitCost,
    ParentSellPrice,
    ParentTax,
    ParentTaxRate,
    ParentTotalIncTax,
    ParentWeight,
    ParentBinRack,
    LocationId,
    ParentItemSource,
    ParentStockItemId,
    ItemId AS SubItemId,
    Title AS SubItemTitle,
    SKU AS SubItemSKU,
    Quantity AS SubItemQty,
    UnitCost AS SubItemUnitCost,
    PricePerUnit AS SubItemSellPrice,
    Tax AS SubItemTax,
    TaxRate AS SubItemTaxRate,
    CostIncTax AS SubItemTotalIncTax,
    Weight AS SubItemWeight,
    BinRack AS SubItemBinRack,
    ItemSource AS SubItemItemSource,
    StockItemId AS SubItemStockItemId
FROM SubItems;
