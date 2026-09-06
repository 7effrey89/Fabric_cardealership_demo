section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "api_products_raw_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared api_products_raw = let
    Source = Json.Document(Web.Contents("https://dummyjson.com/products?limit=100")),
    Products = Source[products],
  #"Converted to table" = Table.FromList(Products, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table", "Column1", {"id", "title", "description", "category", "price", "discountPercentage", "rating", "stock", "tags", "brand", "sku", "weight", "dimensions", "warrantyInformation", "shippingInformation", "availabilityStatus", "reviews", "returnPolicy", "minimumOrderQuantity", "meta", "images", "thumbnail"}, {"id", "title", "description", "category", "price", "discountPercentage", "rating", "stock", "tags", "brand", "sku", "weight", "dimensions", "warrantyInformation", "shippingInformation", "availabilityStatus", "reviews", "returnPolicy", "minimumOrderQuantity", "meta", "images", "thumbnail"}),
    Selected = Table.SelectColumns(#"Expanded Column1", {"id", "title", "description", "category", "brand", "price", "discountPercentage", "rating", "stock", "sku", "availabilityStatus"}, MissingField.UseNull),
    Typed = Table.TransformColumnTypes(Selected, {{"id", Int64.Type}, {"title", type text}, {"description", type text}, {"category", type text}, {"brand", type text}, {"price", Currency.Type}, {"discountPercentage", type number}, {"rating", type number}, {"stock", Int64.Type}, {"sku", type text}, {"availabilityStatus", type text}}),
    WithSource = Table.AddColumn(Typed, "source_system", each "DummyJSON Products API", type text),
    WithIngestion = Table.AddColumn(WithSource, "ingestion_utc", each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()), type datetime),
    WithBatch = Table.AddColumn(WithIngestion, "batch_id", each DateTime.ToText([ingestion_utc], "yyyyMMddHHmmss"), type text)
in
    WithBatch;
shared api_products_raw_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "69eb0045-4ad5-4d4f-b8e1-bd8076532719"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "535f34b5-1f64-47d5-9046-aaf246e3ae82"]}[Data],
  TableNavigation = Navigation_2{[Id = "src_api_product_df", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
