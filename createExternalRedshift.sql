-- Redshift External tables
-- DROP SCHEMA cc_external;
create external schema cc_external
from data catalog 
database 'dev' 
iam_role 'arn:aws:iam::526892378978:role/redshiftaccesstos3'
create external database if not exists;


drop table cc_external.repair_order;
drop table cc_external.ro_damage_line;
drop table cc_external.ro_event;
drop table cc_external.ro_note;
drop table cc_external.ro_total_info;
drop table cc_external.ro_rate_info;
drop table cc_external.opportunity;
drop table cc_external.opp_damage_line;
drop table cc_external.opp_event;
drop table cc_external.opp_note;
drop table cc_external.opp_total_info;
drop table cc_external.opp_rate_info;
drop table cc_external.ro_invoice;
drop table cc_external.ro_invoice_detail;
drop table cc_external.ro_purchase_order;
drop table cc_external.ro_purchase_order_detail;
drop table cc_external.ro_receipts;
drop table cc_external.ro_labor_assignment;
drop table cc_external.ro_credit_memo;
drop table cc_external.ro_credit_memo_detail;


select * from cc_external.repair_order;
select * from cc_external.ro_damage_line;
select * from cc_external.ro_event;
select * from cc_external.ro_note;
select * from cc_external.ro_total_info;
select * from cc_external.ro_rate_info;
select * from cc_external.opportunity;
select * from cc_external.opp_damage_line;
select * from cc_external.opp_event;
select * from cc_external.opp_note;
select * from cc_external.opp_total_info;
select * from cc_external.opp_rate_info;
select * from cc_external.ro_invoice;
select * from cc_external.ro_invoice_detail;
select * from cc_external.ro_purchase_order;
select * from cc_external.ro_purchase_order_detail;
select * from cc_external.ro_receipts;
select * from cc_external.ro_labor_assignment;
select * from cc_external.ro_credit_memo;
select * from cc_external.ro_credit_memo_detail;






create external table cc_external.repair_order(
 repair_order_num           varchar(50),
 estimate_doc_id            varchar(50),
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
 dw_modified_by             varchar(50) )
row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/repair_order/repair_order/'
table properties ('skip.header.line.count'='1');


-- Redshift External ro_damage_line
create external table cc_external.ro_damage_line(
 repair_order_num              varchar(50),
 estimate_doc_id               varchar(50),
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
 dw_modified_by                varchar(50) )
row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/repair_order/ro_damage_line/'
table properties ('skip.header.line.count'='1'); 
 


-- Redshift External ro_event
create external table cc_external.ro_event(
 repair_order_num  varchar(50),
 estimate_doc_id   varchar(50),
 event_typ         varchar(50),
 event_ts          timestamp,
 event_note        varchar(4000),
 event_authored_by varchar(255),
 src_created_ts    timestamp,  
 dw_created_ts     timestamp,
 dw_created_by     varchar(50),
 dw_modified_ts    timestamp,
 dw_modified_by    varchar(50) )
row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/repair_order/ro_event/'
table properties ('skip.header.line.count'='1'); 


-- Redshift External ro_note
create external table cc_external.ro_note(
 repair_order_num     varchar(50),
 estimate_doc_id      varchar(50),
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
 dw_modified_by       varchar(50) )
row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/repair_order/ro_note/'
table properties ('skip.header.line.count'='1'); 


-- Redshift External ro_rate_info
create external table cc_external.ro_rate_info(
 repair_order_num    varchar(50),
 estimate_doc_id     varchar(50),
 rate_typ            varchar(50),
 rate_desc           varchar(255),
 tax_typ             varchar(50),
 taxable_ind         char(1),
 tier_num            smallint,
 rate_val            decimal,
 rate_pct            decimal,
 taxtierinfo_num     integer,
 taxtierinfo_pct     decimal,
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
 dw_modified_by      varchar(50) )
row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/repair_order/ro_rate_info/'
table properties ('skip.header.line.count'='1'); 


-- Redshift External ro_total_info
create external table cc_external.ro_total_info(
 repair_order_num  varchar(50),
 estimate_doc_id   varchar(50),
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
 dw_modified_by  varchar(50) )
 row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/repair_order/ro_total_info/'
table properties ('skip.header.line.count'='1'); 

--opportunity tables
create external table cc_external.opportunity(
 repair_order_num           varchar(50),
 estimate_doc_id            varchar(50),
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
 dw_modified_by             varchar(50) )
row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/opportunity/opportunity/'
table properties ('skip.header.line.count'='1');


-- Redshift External ro_damage_line
create external table cc_external.opp_damage_line(
 repair_order_num              varchar(50),
 estimate_doc_id               varchar(50),
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
 dw_modified_by                varchar(50) )
row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/opportunity/opp_damage_line/'
table properties ('skip.header.line.count'='1'); 
 


-- Redshift External ro_event
create external table cc_external.opp_event(
 repair_order_num  varchar(50),
 estimate_doc_id   varchar(50),
 event_typ         varchar(50),
 event_ts          timestamp,
 event_note        varchar(4000),
 event_authored_by varchar(255),
 src_created_ts    timestamp,  
 dw_created_ts     timestamp,
 dw_created_by     varchar(50),
 dw_modified_ts    timestamp,
 dw_modified_by    varchar(50) )
row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/opportunity/opp_event/'
table properties ('skip.header.line.count'='1'); 


-- Redshift External ro_note
create external table cc_external.opp_note(
 repair_order_num     varchar(50),
 estimate_doc_id      varchar(50),
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
 dw_modified_by       varchar(50) )
row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/opportunity/opp_note/'
table properties ('skip.header.line.count'='1'); 


-- Redshift External ro_rate_info
create external table cc_external.opp_rate_info(
 repair_order_num    varchar(50),
 estimate_doc_id     varchar(50),
 rate_typ            varchar(50),
 rate_desc           varchar(255),
 tax_typ             varchar(50),
 taxable_ind         char(1),
 tier_num            smallint,
 rate_val            decimal,
 rate_pct            decimal,
 taxtierinfo_num     integer,
 taxtierinfo_pct     decimal,
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
 dw_modified_by      varchar(50) )
row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/opportunity/opp_rate_info/'
table properties ('skip.header.line.count'='1'); 


-- Redshift External ro_total_info
create external table cc_external.opp_total_info(
repair_order_num  varchar(50),
estimate_doc_id   varchar(50),
supplier_nm       varchar(250),
supplier_ref_num  varchar(250),
terms_pct         decimal,
terms_memo        varchar(255),
processed_ts      timestamp,
processed_by_first_nm  varchar(250),
processed_by_last_nm   varchar(250),
src_created_ts    timestamp,  
dw_created_ts     timestamp,
dw_created_by     varchar(50),
dw_modified_ts    timestamp,
dw_modified_by    varchar(50))
 row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/opportunity/opp_total_info/'
table properties ('skip.header.line.count'='1'); 











--------------------------------------------------------
--------------------------------------------------------
----
----        PROCUREMENT  TABLES                     ----
--------------------------------------------------------
--------------------------------------------------------





create external table cc_external.ro_purchase_order(
repair_order_num  varchar(50),
estimate_doc_id   varchar(50),
supplier_nm       varchar(250),
supplier_ref_num  varchar(250),
terms_pct         decimal,
terms_memo        varchar(255),
processed_ts      timestamp,
processed_by_first_nm  varchar(250),
processed_by_last_nm   varchar(250),
src_created_ts    timestamp,  
dw_created_ts     timestamp,
dw_created_by     varchar(50),
dw_modified_ts    timestamp,
dw_modified_by    varchar(50))
 row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/purchase_order/ro_purchase_order/'
table properties ('skip.header.line.count'='1'); 






create external table cc_external.ro_purchase_order_detail
(
repair_order_num   varchar(50),
estimate_doc_id    varchar(50),
po_ref_num         varchar(250),
po_ref_line_num    integer,
po_unique_seq_num  integer,
part_num           varchar(250),
part_typ           varchar(50),
part_desc          varchar(250),
part_qty           integer,
part_unit_list_price decimal,
part_unit_net_price  decimal,
request_hold_ind   varchar(10),
src_created_ts     timestamp,  
dw_created_ts      timestamp,
dw_created_by      varchar(50),
dw_modified_ts     timestamp,
dw_modified_by     varchar(50))
 row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/purchase_order/ro_purchase_order_detail/'
table properties ('skip.header.line.count'='1'); 








create external table cc_external.ro_invoice
(
repair_order_num  varchar(50),
estimate_doc_id   varchar(50),
po_ref_num        varchar(250),
supplier_nm       varchar(250),
supplier_ref_num  varchar(250),
terms_pct         decimal,
terms_memo        varchar(255),
invoice_ts        timestamp, 
invoice_tot_amt   decimal,
invoice_tax_tot_amt decimal,
processed_ts      timestamp,
processed_by_first_nm  varchar(250),
processed_by_last_nm   varchar(250),
src_created_ts    timestamp,  
dw_created_ts     timestamp,
dw_created_by     varchar(50),
dw_modified_ts    timestamp,
dw_modified_by    varchar(50))
 row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/invoice/ro_invoice/'
table properties ('skip.header.line.count'='1'); 



create external table cc_external.ro_invoice_detail
(
repair_order_num   varchar(50),
estimate_doc_id    varchar(50),
po_ref_num         varchar(250),
po_ref_line_num    integer,
supplier_ref_line_num  integer,
supplier_response_cd  varchar(250),
part_num           varchar(250),
part_typ           varchar(50),
part_desc          varchar(250),
fulfilled_qty           integer,
part_unit_list_price decimal,
part_unit_net_price  decimal,
request_hold_ind   varchar(10),
src_created_ts     timestamp,  
dw_created_ts      timestamp,
dw_created_by      varchar(50),
dw_modified_ts     timestamp,
dw_modified_by     varchar(50))
 row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/invoice/ro_invoice_detail/'
table properties ('skip.header.line.count'='1'); 






create external table cc_external.ro_credit_memo
(
repair_order_num  varchar(50),
estimate_doc_id   varchar(50),
po_ref_num        varchar(250),
supplier_nm       varchar(250),
supplier_ref_num  varchar(250),
processed_ts      timestamp,
processed_by_first_nm  varchar(250),
processed_by_last_nm   varchar(250),
return_reason_cd  varchar(250),
invoice_ref_num      varchar(250),
credit_memo_num  varchar(250),
credit_unit_net_price   decimal,
src_created_ts    timestamp,  
dw_created_ts     timestamp,
dw_created_by     varchar(50),
dw_modified_ts    timestamp,
dw_modified_by    varchar(50))
 row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/credit_memo/ro_credit_memo/'
table properties ('skip.header.line.count'='1'); 


     

create external table cc_external.ro_credit_memo_detail
(
repair_order_num  varchar(50),	 
estimate_doc_id    varchar(50),
po_ref_num         varchar(250),
po_ref_line_num    integer,
part_typ           varchar(50),
part_desc          varchar(250),
part_quantity           integer,
part_unit_net_price  decimal,
request_hold_ind   varchar(10),
src_created_ts     timestamp,  
dw_created_ts      timestamp,
dw_created_by      varchar(50),
dw_modified_ts     timestamp,
dw_modified_by     varchar(50))
 row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/credit_memo/ro_credit_memo_detail/'
table properties ('skip.header.line.count'='1'); 





create external table cc_external.ro_receipts
(
repair_order_num  varchar(50),  
estimate_doc_id   varchar(50),
payer_typ         varchar(250),
payment_typ       varchar(250),
payment_ts        timestamp,
payment_amt       decimal,
payment_id        varchar(250),
payment_memo      varchar(400),
src_created_ts    timestamp,  
dw_created_ts     timestamp,
dw_created_by     varchar(50),
dw_modified_ts    timestamp,
dw_modified_by    varchar(50))
 row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/receipts/ro_receipts/'
table properties ('skip.header.line.count'='1'); 




create external table cc_external.ro_labor_assignment
(
repair_order_num  varchar(50),
estimate_doc_id   varchar(50),
labor_typ         varchar(250),
team_nm           varchar(250),
team_desc         varchar(250),
team_num          integer,
allocated_hrs     decimal,
allocated_ts      timestamp,
allocated_by_first_nm  varchar(250),
allocated_by_last_nm   varchar(250),
src_created_ts    timestamp,  
dw_created_ts     timestamp,
dw_created_by     varchar(50),
dw_modified_ts    timestamp,
dw_modified_by    varchar(50))
 row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/labor_assignment/ro_labor_assignment/'
table properties ('skip.header.line.count'='1'); 














create external table cc_external.log_table(
 repair_order_num  varchar(50),
 estimate_doc_id   varchar(50),
 src_created_ts    timestamp,  
 file_name         varchar(255),
 event_topic       varchar(50) )
row format delimited
fields terminated by '|'
stored as textfile
location 's3://pipedelimfiles-for-table/log_table/'
table properties ('skip.header.line.count'='1'); 
