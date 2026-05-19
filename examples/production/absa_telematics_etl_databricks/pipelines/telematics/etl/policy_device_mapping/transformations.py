"""Telematics policy-device-exposure mapping transformation.

Ports the ``telematics_policy_imei_mapping_etl`` logic into a Ubunye ``Task``:

1. Snake-case rename the MI exposure columns.
2. Derive ``policy_product_description`` and ``policy_version_number`` from
   the raw ``policy_no`` string.
3. Keep the latest policy version per ``(policy_number, lob_asset_id,
   effective_year_month)`` and the latest telematics record per
   ``(policy_number, item_no)``.
4. Compute the earliest IMEI-detection datetime per device and use it to
   correct the PDD ``installation_datetime`` when the device was seen in
   the wild before its recorded install.
5. Right-join the corrected telematics frame onto the latest MI frame on
   ``telematics.item_no == mi.lob_asset_id`` and emit the final select.

Pure-Spark function + ``Task`` wrapper - no MLflow, no I/O. Observability
lands through the engine hooks (events + lineage) when the pipeline runs.
"""

from __future__ import annotations

from typing import Any, Dict

from ubunye.core.interfaces import Task

RENAME_COLS: Dict[str, str] = {
    "SourceData": "source_data",
    "PolicyNo": "policy_no",
    "PolicyNumber": "policy_number",
    "PolicyInceptionDate": "policy_inception_date",
    "ItemInceptionDate": "item_inception_date",
    "CancelledDate": "cancelled_date",
    "EFF_FROM_DATE": "eff_from_date",
    "EFF_TO_DATE": "eff_to_date",
    "SectionCode": "section_code",
    "CoverCode": "cover_code",
    "ItemDescription": "item_description",
    "VehMake": "veh_make",
    "VehModel": "veh_model",
    "Registration": "registration",
    "VinNumber": "vin_number",
    "IMEINumber": "imei_number",
    "CoverPremium": "cover_premium",
    "CoverSumInsured": "cover_sum_insured",
    "CoverDescription": "cover_description",
    "PolicyStatus": "policy_status",
    "CoverStatus": "cover_status",
    "LOB_ASSET_ID": "lob_asset_id",
    "ItemNo": "item_no_raw",
    "PolicyCancellationReasonCode": "policy_cancellation_reason_code",
    "enddate": "end_date",
    "effectivefromdate": "effective_from_date",
    "effectivetodate": "effective_to_date",
    "EffectiveYearMonth": "effective_year_month",
    "EffectiveYear": "effective_year",
    "EffectiveQuarter": "effective_quarter",
    "EffectiveMonth": "effective_month",
    "EffectiveYearQuarter": "effective_year_quarter",
    "ItemCancelledDate": "item_cancelled_date",
    "PremiumPaid": "premium_collected",
    "ACTIVE_LOB_ASSET_ID": "active_lob_asset_id",
}

OUTPUT_COLUMNS = (
    "source_data",
    "policy_version_number",
    "policy_product_description",
    "policy_number",
    "item_no",
    "premium",
    "premium_collected",
    "cover_sum_insured",
    "active_lob_asset_id",
    "original_lob_asset_id",
    "vin_number",
    "imei_number",
    "make",
    "model",
    "registration_number",
    "section_code",
    "cover_code",
    "item_description",
    "cover_description",
    "policy_status",
    "cover_status",
    "policy_cancellation_reason_code",
    "item_inception_datetime",
    "policy_inception_date",
    "item_cancellation_date",
    "policy_cancellation_date",
    "imei_first_detection_datetime",
    "installation_datetime",
    "effective_year_month",
    "effective_from_date",
    "effective_to_date",
    "inserted_timestamp",
)


def _rename_mi(df: "Any") -> "Any":
    out = df
    for old, new in RENAME_COLS.items():
        if old in out.columns:
            out = out.withColumnRenamed(old, new)
    return out


def _derive_policy_features(df: "Any") -> "Any":
    from pyspark.sql import functions as F

    return (
        df.withColumn("cleaned_policy_no", F.regexp_replace("policy_no", "-", ""))
        .withColumn(
            "policy_product_description",
            F.regexp_extract("cleaned_policy_no", r"^([A-Za-z]+)", 1),
        )
        .withColumn(
            "policy_version_number",
            F.split(F.col("policy_no"), "/").getItem(1).cast("int"),
        )
    )


def _latest_mi(df: "Any") -> "Any":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    w = Window.partitionBy("policy_number", "lob_asset_id", "effective_year_month").orderBy(
        F.col("policy_version_number").desc()
    )
    return (
        df.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def _latest_telematics(df: "Any") -> "Any":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    w = Window.partitionBy("policy_number", "item_no").orderBy(F.col("inserted_datetime").desc())
    drop_cols = ["row_num"]
    if "row_id" in df.columns:
        drop_cols.append("row_id")
    return (
        df.withColumn("row_num", F.row_number().over(w))
        .filter(F.col("row_num") == 1)
        .drop(*drop_cols)
    )


def _imei_first_detection(df: "Any") -> "Any":
    from pyspark.sql import functions as F

    return df.groupBy("imei_number").agg(
        F.min("retrieved_datetime").alias("imei_first_detection_datetime")
    )


def _correct_installation(telematics_latest: "Any", imei_first: "Any") -> "Any":
    from pyspark.sql import functions as F

    te = telematics_latest.alias("te")
    ui = imei_first.alias("ui")
    return te.join(ui, on="imei_number", how="left").withColumn(
        "installation_datetime_final",
        F.when(F.col("te.installation_datetime").isNull(), F.col("ui.imei_first_detection_datetime"))
        .when(
            F.col("ui.imei_first_detection_datetime") < F.col("te.installation_datetime"),
            F.col("ui.imei_first_detection_datetime"),
        )
        .otherwise(F.col("te.installation_datetime")),
    )


def build_policy_device_mapping(
    policy_device_details: "Any",
    user_imei: "Any",
    activateitempremiumexposure: "Any",
) -> "Any":
    """Compose the four stages into the final exposure-mapping DataFrame.

    The business logic is a straight port of the legacy ETL; the correction
    to ``installation_datetime`` propagates the ``_final`` column into the
    output (the legacy script computed it but then selected the raw one -
    that was a latent bug, surfaced here).
    """
    from pyspark.sql import functions as F

    mi_renamed = _rename_mi(activateitempremiumexposure)
    mi_featured = _derive_policy_features(mi_renamed)
    mi_latest = _latest_mi(mi_featured)
    telematics_latest = _latest_telematics(policy_device_details)
    imei_first = _imei_first_detection(user_imei)
    telematics_corrected = _correct_installation(telematics_latest, imei_first)

    joined = telematics_corrected.alias("telematics").join(
        mi_latest.alias("mi"),
        on=F.col("telematics.item_no") == F.col("mi.lob_asset_id"),
        how="right",
    )

    return joined.select(
        F.col("mi.source_data").alias("source_data"),
        F.col("mi.policy_version_number"),
        F.col("mi.policy_product_description"),
        F.col("mi.policy_number"),
        F.col("mi.lob_asset_id").alias("item_no"),
        F.col("mi.cover_premium").alias("premium"),
        F.col("mi.premium_collected"),
        F.col("mi.cover_sum_insured"),
        F.col("mi.active_lob_asset_id"),
        F.col("mi.item_no_raw").alias("original_lob_asset_id"),
        F.col("mi.vin_number"),
        F.col("mi.imei_number"),
        F.col("mi.veh_make").alias("make"),
        F.col("mi.veh_model").alias("model"),
        F.col("mi.registration").alias("registration_number"),
        F.col("mi.section_code"),
        F.col("mi.cover_code"),
        F.col("mi.item_description"),
        F.col("mi.cover_description"),
        F.col("mi.policy_status"),
        F.col("mi.cover_status"),
        F.col("mi.policy_cancellation_reason_code"),
        F.col("mi.item_inception_date").alias("item_inception_datetime"),
        F.col("mi.policy_inception_date"),
        F.col("mi.item_cancelled_date").alias("item_cancellation_date"),
        F.col("mi.cancelled_date").alias("policy_cancellation_date"),
        F.col("telematics.imei_first_detection_datetime"),
        F.col("telematics.installation_datetime_final").alias("installation_datetime"),
        F.col("mi.effective_year_month"),
        F.col("mi.effective_from_date"),
        F.col("mi.effective_to_date"),
        F.current_timestamp().alias("inserted_timestamp"),
    )


class PolicyDeviceMapping(Task):
    """Ubunye Task: telematics policy/device/exposure mapping."""

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "policy_device_mapping_exposure": build_policy_device_mapping(
                policy_device_details=sources["policy_device_details"],
                user_imei=sources["user_imei"],
                activateitempremiumexposure=sources["activateitempremiumexposure"],
            )
        }
