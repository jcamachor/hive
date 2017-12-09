-- create mv_creation_metadata table
CREATE TABLE "APP"."MV_CREATION_METADATA" ("TBL_ID" BIGINT NOT NULL, "TBL_NAME" VARCHAR(256) NOT NULL, "LAST_TRANSACTION_INFO" LONG VARCHAR NOT NULL);
ALTER TABLE "APP"."MV_CREATION_METADATA" ADD CONSTRAINT "MV_CREATION_METADATA_FK" FOREIGN KEY ("TBL_ID") REFERENCES "APP"."TBLS" ("TBL_ID") ON DELETE NO ACTION ON UPDATE NO ACTION;

-- modify completed_txn_components table
RENAME TABLE "COMPLETED_TXN_COMPONENTS" TO "COMPLETED_TXN_COMPONENTS_BACKUP";
CREATE TABLE "COMPLETED_TXN_COMPONENTS" (
  "CTC_TXNID" bigint,
  "CTC_DATABASE" varchar(128) NOT NULL,
  "CTC_TABLE" varchar(256),
  "CTC_PARTITION" varchar(767),
  "CTC_ID" bigint GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) NOT NULL,
  "CTC_TIMESTAMP" timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
);
CREATE INDEX "APP"."COMPLETED_TXN_COMPONENTS_IDX" ON "APP"."COMPLETED_TXN_COMPONENTS" ("CTC_ID");
CREATE INDEX "APP"."COMPLETED_TXN_COMPONENTS_IDX2" ON "APP"."COMPLETED_TXN_COMPONENTS" ("CTC_DATABASE", "CTC_TABLE", "CTC_PARTITION");
INSERT INTO "COMPLETED_TXN_COMPONENTS" ("CTC_TXNID", "CTC_DATABASE", "CTC_TABLE", "CTC_PARTITION")
SELECT "CTC_TXNID", "CTC_DATABASE", "CTC_TABLE", "CTC_PARTITION" FROM "COMPLETED_TXN_COMPONENTS_BACKUP";
DROP TABLE "APP"."COMPLETED_TXN_COMPONENTS_BACKUP";
