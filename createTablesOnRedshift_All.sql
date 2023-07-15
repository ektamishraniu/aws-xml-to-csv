--CREATE SCHEMA cc_staging; 

drop table cc_staging.repair_order cascade;
CREATE TABLE cc_staging.repair_order
(
 repair_order_num           varchar(50) NOT NULL,
 estimate_doc_id            varchar(50) NOT NULL,
 profile_nm                 varchar(50),
 claim_num                  varchar(50),
 policy_num                 varchar(50),
 estimate_sts               varchar(50),
 repair_order_typ           varchar(50),
 referral_src_nm            varchar(255),
 vendor_cd                  varchar(50),
 loss_desc                  varchar(50),
 tot_loss_ind               char(1),
 drivable_ind               char(1),
 loss_primary_poi_cd        varchar(50),
 loss_primary_poi_desc      varchar(255),
 loss_secondary_poi_cd      varchar(50),
 loss_secondary_poi_desc    varchar(255),
 loss_occr_state            varchar(50),
 loss_ts                    timestamp,
 loss_reported_ts           timestamp,
 ro_created_ts              timestamp,
 estimate_appt_ts           timestamp,
 schd_arrival_ts            timestamp,
 tgt_start_dt               date,
 tgt_compl_ts               timestamp,
 requested_pickup_ts        timestamp,
 arrival_ts                 timestamp,
 arrival_start_dt           date,
 actual_compl_ts            timestamp,
 actual_pickup_ts           timestamp,
 close_ts                   timestamp,
 rental_assisted_ind        char(1),
 arrival_odometer_reading   varchar(50),
 departure_odometer_reading varchar(50),
 vehicle_vin                varchar(50),
 vehicle_licesne_plate_num  varchar(50),
 vehicle_licesne_state      varchar(50),
 vehicle_model_yr           varchar(50),
 vehicle_body_style         varchar(50),
 vehicle_trim_cd            varchar(50),
 vehicle_production_dt      date,
 vehicle_make_desc          varchar(50),
 vehicle_model_nm           varchar(255),
 vehicle_ext_color_nm       varchar(50),
 vehicle_ext_color_cd       varchar(50),
 vehicle_int_color_nm       varchar(50),
 vehicle_int_color_cd       varchar(50),
 injury_ind                 varchar(50),
 production_stage_cd        varchar(50),
 production_stage_dt        date,
 production_stage_sts_txt   varchar(1000),
 src_created_ts             timestamp,   
 dw_created_ts              timestamp,
 dw_created_by              varchar(50),
 dw_modified_ts             timestamp,
 dw_modified_by             varchar(50),
 CONSTRAINT PK_repair_order PRIMARY KEY ( repair_order_num )
);


-- *********************** SqlDBM: Redshift ************************
-- ******************************************************************
-- ************************************** ro_damage_line
drop table cc_staging.ro_damage_line cascade;
CREATE TABLE cc_staging.ro_damage_line
(
 repair_order_num              varchar(50) NOT NULL,
 line_num                      varchar(50),
 unique_seq_num                varchar(50),
 supplement_num                varchar(50),
 estimate_ver_cd               varchar(50),
 manual_line_ind               char(1),
 automated_entry               varchar(50),
 line_sts_cd                   varchar(50),
 message_cd                    varchar(50),
 vendor_ref_num                varchar(50),
 line_desc                     varchar(255),
 desc_judgement_ind            char(1),
 part_typ                      varchar(50),
 part_num                      varchar(50),
 oem_part_num                  varchar(50),
 part_price                    varchar(50),
 unit_part_price               varchar(50),
 part_price_adj_typ            varchar(50),
 part_price_adj_pct            varchar(50),
 part_price_adj_amt            varchar(50),
 part_taxable_ind              char(1),
 price_judgement_ind           char(1),
 alternate_part_ind            char(1),
 glass_part_ind                char(1),
 part_price_incl_ind           char(1),
 part_quantity                 varchar(50),
 labor_typ                     varchar(50),
 labor_operation               varchar(50),
 actual_labor_hrs              varchar(50),
 db_labor_hrs                  varchar(50),
 labor_incl_ind                char(1),
 labor_amt                     decimal,
 labor_hrs_judgement_ind       char(1),
 refinish_labor_typ            varchar(50),
 refinish_labor_operation      varchar(50),
 refinish_labor_hrs            varchar(50),
 refinish_db_labor_hrs         varchar(50),
 refinish_labor_incl_ind       char(1),
 refinish_labor_amt            decimal,
 refinish_labor_hrs_jdgmnt_ind varchar(50),
 oth_charges_typ               varchar(50),
 oth_charges_price             varchar(50),
 oth_charges_uom               varchar(50),
 oth_charges_qty               varchar(50),
 oth_charges_price_incl_ind    char(1),
 line_memo                     varchar(1000),
 src_created_ts                timestamp,   
 dw_created_ts                 timestamp,
 dw_created_by                 varchar(50),
 dw_modified_ts                timestamp,
 dw_modified_by                varchar(50),
 CONSTRAINT FK_166 FOREIGN KEY ( repair_order_num ) REFERENCES repair_order ( repair_order_num )
);


-- *********************** SqlDBM: Redshift ************************
-- ******************************************************************
-- ************************************** ro_event
CREATE TABLE cc_staging.ro_event
(
 repair_order_num  varchar(50) NOT NULL,
 event_typ         varchar(50),
 event_ts          timestamp,
 event_note        varchar(4000),
 event_authored_by varchar(255),
 src_created_ts    timestamp,  
 dw_created_ts     timestamp,
 dw_created_by     varchar(50),
 dw_modified_ts    timestamp,
 dw_modified_by    varchar(50),
 CONSTRAINT FK_156 FOREIGN KEY ( repair_order_num ) REFERENCES repair_order ( repair_order_num )
);



-- *********************** SqlDBM: Redshift ************************
-- ******************************************************************
-- ************************************** ro_note
drop table cc_staging.ro_note;
CREATE TABLE cc_staging.ro_note
(
 repair_order_num     varchar(50) NOT NULL,
 line_seq_num         varchar(50),
 note_grp             varchar(50),
 note_created_ts      timestamp,
 note                 varchar(max),
 authored_by_first_nm varchar(255),
 authored_by_last_nm  varchar(255),
 authored_by_alias    varchar(255),
 emp_id               bigint,
 src_created_ts       timestamp,  
 dw_created_ts        timestamp,
 dw_created_by        varchar(50),
 dw_modified_ts       timestamp,
 dw_modified_by       varchar(50),
 CONSTRAINT FK_159 FOREIGN KEY ( repair_order_num ) REFERENCES repair_order ( repair_order_num )
);


-- *********************** SqlDBM: Redshift ************************
-- ******************************************************************
-- ************************************** ro_rate_info
drop table cc_staging.ro_rate_info cascade;
CREATE TABLE cc_staging.ro_rate_info
(
 repair_order_num    varchar(50) NOT NULL,
 rate_typ            varchar(50),
 rate_desc           varchar(255),
 tax_typ             varchar(50),
 taxable_ind         char(1),
 tier_num            smallint,
 rate_val            decimal,
 rate_pct            decimal,
 taxtierinfo_num     integer,
 taxtierinfo_pct     integer,
 taxtierinfo_threshold_amt       decimal,
 taxtierinfo_surcharge_amt       decimal,
 adjustinfo_adjust_pct          decimal,
 adjustinfo_adjust_typ          varchar(50),
 matr_calc_method_cd varchar(50),
 matr_calc_max_amt   decimal,
 matr_calc_max_hrs   decimal,
 src_created_ts      timestamp,  
 dw_created_ts       timestamp,
 dw_created_by       varchar(50),
 dw_modified_ts      timestamp,
 dw_modified_by      varchar(50),
 CONSTRAINT FK_153 FOREIGN KEY ( repair_order_num ) REFERENCES repair_order ( repair_order_num )
);


-- *********************** SqlDBM: Redshift ************************
-- ******************************************************************
-- ************************************** ro_total_info
drop table cc_staging.ro_total_info cascade;
CREATE TABLE cc_staging.ro_total_info
(
 repair_order_num  varchar(50) NOT NULL,
 tot_typ_ctgry   varchar(50),
 tot_typ         varchar(50),
 tot_sub_typ     varchar(20),
 tot_typ_desc    varchar(50),
 tot_hrs         numeric(8,2),
 tot_amt         numeric(8,2),
 tot_cost        numeric(8,2),
 tot_adj_typ     varchar(50),
 tot_adj_amt     numeric(8,2),
 src_created_ts  timestamp,   
 dw_created_ts   timestamp,
 dw_created_by   varchar(50),
 dw_modified_ts  timestamp,
 dw_modified_by  varchar(50),
 CONSTRAINT FK_163 FOREIGN KEY ( repair_order_num ) REFERENCES repair_order ( repair_order_num )
);