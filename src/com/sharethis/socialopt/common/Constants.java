package com.sharethis.socialopt.common;

import java.util.HashMap;

public class Constants {
	public static final String COLLECT_RT_LOGGER_NAME = "rt.conversions";
	public static final String COLLECT_DLX_LOGGER_NAME = "rtb.dlx";
	public static final String COLLECT_DATASET_LOGGER_NAME = "modeling.dataset";
	public static final String COLLECT_MOAT_LOGGER_NAME = "moat";
	public static final int NUM_REDUCER = 500;
	public static final int NUM_DLX_DIM = 17;
	
	public static final HashMap<String, String> HM_HOUR_GROUP;
	static {
		HM_HOUR_GROUP = new HashMap<String, String>();
		HM_HOUR_GROUP.put("07", "1");
		HM_HOUR_GROUP.put("08", "1");
		HM_HOUR_GROUP.put("09", "1");
		HM_HOUR_GROUP.put("10", "1");
		HM_HOUR_GROUP.put("11", "1");
		HM_HOUR_GROUP.put("12", "1");
		HM_HOUR_GROUP.put("13", "1");
		HM_HOUR_GROUP.put("14", "2");
		HM_HOUR_GROUP.put("15", "2");
		HM_HOUR_GROUP.put("16", "2");
		HM_HOUR_GROUP.put("17", "2");
		HM_HOUR_GROUP.put("18", "3");
		HM_HOUR_GROUP.put("19", "3");
		HM_HOUR_GROUP.put("20", "3");
		HM_HOUR_GROUP.put("21", "3");
		HM_HOUR_GROUP.put("22", "3");
		HM_HOUR_GROUP.put("23", "3");
		HM_HOUR_GROUP.put("00", "3");
		HM_HOUR_GROUP.put("01", "4");
		HM_HOUR_GROUP.put("02", "4");
		HM_HOUR_GROUP.put("03", "4");
		HM_HOUR_GROUP.put("04", "4");
		HM_HOUR_GROUP.put("05", "4");
		HM_HOUR_GROUP.put("06", "4");
	};
	
	public static final String[] FIELDS = {
		"bid", "wp_bucket", "ad_slot_id", "visibility", "geo_location", 
		"google_id", "setting_id", "user_seg_list", "verticle_list", "date", 
		"timestamp", "campaign_name", "jid", "campaign_id", "adgroup_id", 
		"creative_id", "domain", "deal_id", "ip", "user_agent", 
		"cookie", "service_type", "st_campaign_id", "st_adgroup_id", "st_creative_id", 
		"click_flag","imp_flag", "click_timestamp", "min_bid", "platform_type", 
		"geo_target_id", "mobile", "model_id", "model_score", "gid_age", 
		"audience_id", "browser", "os", "device", "creative_size", 
		"sqi", "err_flag", "flag", "app_id", "carrier_id", 
		"device_type", "is_app", "is_initial_request", "platform", "screen_orientation", 
		"device_id", "location", "device_make", "device_model", "device_id_type", 
		"seller_id", "geofence_id", "geo_lat", "geo_long", "stid_soc",
		"tm_soc", "gid_soc", "gid_ts_soc", "fpc_soc", "lang_soc", 
		"geo_ctry_soc", "dlx_bit_soc", "version_soc","pview_cnt_soc", "search_cnt_soc",
		"share_cnt_soc", "click_cnt_soc", "dmn_cnt_soc", "chnl_cnt_soc", "cat_cnt_soc",
		"events_soc", "dlx_seg_soc", "dmn_list_soc", "chnl_list_soc", "cat_list_soc",
		"event_list_soc", "conv_enabled", "conversion", "site_moat_enabled", "site_moat",
		"ad_moat_enabled", "ad_moat"
	};
	
	
}