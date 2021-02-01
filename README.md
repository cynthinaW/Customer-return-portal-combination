# Customer-return-portal-combination

SELECT orders,
       order_date,REPORTED,SKU,DESCRIPTION, ACTION,STATUS, NOTES_FIELD, REPLACEMENT, LAST_UPDATED, HANDLER, vendor_name, 
       PROBLEM_TYPE, "REQUESTED_REPLACEMENT_DATE" 
       FROM (
select orders, row_number() over (partition by orders order by "source") row_num,
       order_date,REPORTED,SKU,DESCRIPTION, ACTION,STATUS, NOTES_FIELD, REPLACEMENT, LAST_UPDATED, HANDLER, vendor_name, 
       PROBLEM_TYPE, "REQUESTED_REPLACEMENT_DATE"
FROM (

SELECT    innerQ.ORDERS, 
          innerQ.date_created AS order_date, 
          innerQ.REPORTED, 
          innerQ.SKU, 
          innerQ.DESCRIPTION, 
          innerQ.ACTION, 
          innerQ.STATUS, 
          innerQ.NOTES_FIELD, 
          innerQ.REPLACEMENT, 
          innerQ.LAST_UPDATED, 
          innerQ.HANDLER, 
          v.name AS vendor_name, 
          innerQ.PROBLEM_TYPE, 
          innerQ.REP_DATE AS "REQUESTED_REPLACEMENT_DATE",
          1 "source"
FROM      ( 
                           SELECT           pe.order_id_nmb 
                                                             || '-' 
                                                             || pe.shipment_number AS ORDERS, 
                                             pe.create_dt           Reported, 
                                            osk.sku, 
                                            sk.sze 
                                                             || ' ' 
                                                             || sk.color description, 
                                            CASE 
                                                             WHEN s.is_drop_ship=0 THEN 'no' 
                                                             ELSE 'yes' 
                                                                                                      END,
                                            dae.name_txt                                              AS Action,
                                            st.status_txt                                             AS status,
                                            COALESCE(pe.notes_txt, 'ITC HAS NOT LOGGED DO NOT CLOSE')    notes_field,
                                            afr.response_txt                                             Replacement,
                                            TO_CHAR(afr.response_dt, 'MM/DD/YYYY HH:MI AM')              Last_updated,
                                            am1.first_name 
                                                             || ' ' 
                                                             || am1.last_name                                                           AS HANDLER,
                                            ROW_NUMBER() OVER(PARTITION BY afr.problem_id_nmb ORDER BY afr.response_dt DESC NULLS LAST)    rk,
                                            o.date_created, 
                                            ptl.name_txt                                                                                       AS PROBLEM_TYPE,
                                            COALESCE(Substring(urgent_response_join.response_txt,'[[:digit:]]+/[^/]+/[[:digit:]]+'),'NO RUSH') AS REP_DATE,
                                            dnt.node_id_nmb 
                           FROM             shipment s 
                           JOIN             order_shipment os 
                           ON               ( 
                                                             s.shipment_id = os.shipment_id) 
                           JOIN             order_sku osk 
                           ON               ( 
                                                             os.order_sku_id= osk.order_sku_id) 
                           JOIN             sku sk 
                           ON               ( 
                                                             sk.sku = osk.sku) 
                           LEFT OUTER JOIN 
                                            ( 
                                                     SELECT   Problem_ID_Nmb, 
                                                              Action_id_nmb, 
                                                              ROW_NUMBER() OVER(PARTITION BY Problem_id_Nmb ORDER BY Action_id_nmb DESC NULLS LAST) rk
                                                     FROM     DT_Problem_action_relship 
                                                     WHERE    action_id_nmb IN (2, 
                                                                                3, 
                                                                                4, 
                                                                                5, 
                                                                                6, 
                                                                                8)) dpar 
                           ON               ( 
                                                             dpar.rk = 1) 
                           LEFT OUTER JOIN  DT_ACTION_ETY dae 
                           ON               ( 
                                                             dpar.action_id_nmb = dae.id_nmb) 
                           LEFT OUTER JOIN  dt_action_field_response_ety afr 
                           ON               ( 
                                                             afr.field_id_nmb IN (36, 
                                                                                  19, 
                                                                                  22)) 
                           RIGHT OUTER JOIN DT_problem_ety pe 
                           ON               ( 
                                                             dpar.problem_id_nmb = pe.id_nmb 
                                            AND              AFR.PROBLEM_ID_NMB = Pe.ID_NMB) 
                           LEFT OUTER JOIN 
                                            ( 
                                                             SELECT           p.id_nmb, 
                                                                              afr.administrator_ID_NMB                                                                    AS ADMIN,
                                                                              TO_CHAR(afr.response_dt, 'MM/DD/YYYY')                                                         Last_updated,
                                                                              ROW_NUMBER() OVER(PARTITION BY afr.problem_id_nmb ORDER BY afr.response_dt DESC NULLS LAST)    rk
                                                             FROM             dt_action_field_response_ety afr
                                                             RIGHT OUTER JOIN dt_problem_ety p 
                                                             ON               ( 
                                                                                               AFR.PROBLEM_ID_NMB = P.ID_NMB)) ITC
                           ON               ( 
                                                             pe.id_nmb = itc.id_nmb) 
                           LEFT OUTER JOIN  administrators am1 
                           ON               ( 
                                                             itc.ADMIN::text = am1.ID::text) 
                           JOIN             dt_itc_status_lkup st 
                           ON               ( 
                                                             pe.itc_status_id_nmb::text = st.id_nmb::text)
                           LEFT OUTER JOIN 
                                            ( 
                                                            SELECT DISTINCT PNT.PROBLEM_ID_NMB, 
                                                                            BR.RESPONSE_TXT 
                                                            FROM            DT_BOX_QUESTION_ETY bq,
                                                                            DT_BOX_RESPONSE_ETY br,
                                                                            DT_PROBLEM_NODES_TRAVERSED pnt
                                                            WHERE           BR.PROBLEM_NODES_TRAVERSED_ID_NMB = PNT.ID_NMB
                                                            AND             BQ.ID_NMB = BR.BOX_QUESTION_ID_NMB
                                                            AND             BQ.BOX_ID_NMB IN (4,
                                                                                              7)
                                                            AND             BR.BOX_QUESTION_ID_NMB IN (11,
                                                                                                       17)
                                                            AND             BR.ANSWER_CHOICE_ID_NMB IN (36,
                                                                                                        56))urgent_response_join
                           ON               ( 
                                                             pe.id_nmb = urgent_response_join.problem_id_nmb)
                           JOIN             orders o 
                           ON               ( 
                                                             o.order_id = s.order_id) 
                           JOIN             dt_problem_nodes_traversed dnt 
                           ON               ( 
                                                             pe.id_nmb = dnt.problem_id_nmb) 
                           LEFT JOIN        dt_problem_type_lkup ptl 
                           ON               pe.problem_type_id_nmb = ptl.id_nmb 
                           WHERE            pe.order_id_nmb = s.order_id 
                           AND              pe.shipment_number = s.shipment_number 
                           AND              pe.cancelled_bit = 0 
                           AND              s.IS_DROP_SHIP = 1 
                           AND              ITC.rk = 1 
                           AND              COALESCE(pe.notes_txt, ' ') NOT LIKE '%##DISRE##%' 
                                            /*and dnt.node_id_nmb in (1316,1318,1325,1327,1335,1337,1342,1344,1353,1355,1364,1373,1375)*/
                           AND              pe.create_dt::date >= To_date(:date1_bv, 'MM/DD/YYYY')
                           AND              pe.create_dt::date <= To_date(:date2_bv, 'MM/DD/YYYY')) innerQ
LEFT JOIN shipment s 
ON        s.shipment_id = Substring(innerQ.REPLACEMENT,'[[:digit:]]{7}')::INT 
LEFT JOIN orders o 
ON        o.order_id::text = s.order_id::text 
LEFT JOIN SOURCE so 
ON        so.id = o.source_id, 
          vendor v, 
          vendor_sku vs 
WHERE     rk = 1 
AND       innerQ.sku = vs.sku 
AND       vs.VENDOR_ID = v.ID 


union all

select 
             ro.order_id::varchar || '-' || s.shipment_number::varchar orders, s.authorized order_date, 
             ro.created_date as "reported", 
             osk.sku,
             case when sk.sze || ' '|| sk.color like '%&#%' or sk.sze || ' '|| sk.color like '%nbsp%' then sk.sze else sk.sze || ' '|| sk.color end as description,
             case when roh2.notes LIKE '%replacement%' then 'replacement order' end "action",
             rst.name status,
             ros.customer_notes notes_field,
             roh2.order_id::varchar replacement_order_id,
             ro.modified_date::varchar last_updated,
             ad.first_name  || ' '    || ad.last_name                                                           "handler",
             v.name AS vendor_name,
             rosr.reason_name problem_type,
             ' ' "REQUESTED_REPLACEMENT_DATE",
             2 "source"
      from return_order ro inner join return_status rst on ro.return_status_id = rst.id
                           inner join return_order_history roh on roh.return_order_id = ro.return_order_id
                           left join return_order_history roh2 on roh2.return_order_id = ro.return_order_id AND roh2.notes LIKE '%replacement%'
                           inner join administrators ad on ad.id = roh.administrator_id
                           inner join return_order_sku ros on ro.return_order_id = ros.return_order_id
                           LEFT JOIN order_sku osk ON ros.order_sku_id = osk.order_sku_id
                           JOIN sku sk ON sk.sku = osk.sku
                        
                           inner join return_order_type rot on rot.return_order_type_id = ro.return_order_type_id
                           JOIN shipment s  on s.shipment_id = ro.shipment_id
                           JOIN vendor_sku vs ON sk.sku = vs.sku 
                           JOIN vendor v ON vs.VENDOR_ID = v.ID,
                           return_order_sku_reason rosr
      where ros.return_order_sku_reason_id=rosr.return_order_sku_reason_id
      and to_date(To_char(roh.created_date, 'YYYY-MON-DD'),'YYYY-MON-DD') >= To_date(:date1_bv, 'MM/DD/YYYY')
      and to_date(To_char(roh.created_date, 'YYYY-MON-DD'),'YYYY-MON-DD') <= To_date(:date2_bv, 'MM/DD/YYYY')
      and ro.confirmation_email not like '%qa%'
      and ro.return_order_type_id in (1,2,3)
      and roh.notes LIKE '%Zendesk ticket created%'
      AND s.is_drop_ship = 1
      
union all     
      
SELECT       ro.order_id::varchar || '-' || s.shipment_number::varchar orders, s.authorized order_date, 
             ro.created_date as "reported", 
             osk.sku,
             case when sk.sze || ' '|| sk.color like '%&#%' or sk.sze || ' '|| sk.color like '%nbsp%' then sk.sze else sk.sze || ' '|| sk.color end as description,
             case when orders.notes LIKE '%Rpl order%' then 'replacement order' end "action",
             rst.name status,
             ros.customer_notes notes_field,
             substring(orders.notes from '.*order[\s:#]*(\d{7}).*')  replacement_order_id,
             ro.modified_date::varchar last_updated,
             ad.first_name  || ' '    || ad.last_name                                                           "handler",
             v.name AS vendor_name,
             rosr.reason_name problem_type,
             ' ' "REQUESTED_REPLACEMENT_DATE",
             3 "source"
      from return_order ro inner join orders on orders.order_id=ro.order_id
                           inner join return_status rst on ro.return_status_id = rst.id
                           inner join return_order_history roh on roh.return_order_id = ro.return_order_id
                           left join return_order_history roh2 on roh2.return_order_id = ro.return_order_id AND roh2.notes LIKE '%replacement%'
                           inner join administrators ad on ad.id = roh.administrator_id
                           inner join return_order_sku ros on ro.return_order_id = ros.return_order_id
                           LEFT JOIN order_sku osk ON ros.order_sku_id = osk.order_sku_id
                           JOIN sku sk ON sk.sku = osk.sku
                        
                           inner join return_order_type rot on rot.return_order_type_id = ro.return_order_type_id
                           JOIN shipment s  on s.shipment_id = ro.shipment_id
                           JOIN vendor_sku vs ON sk.sku = vs.sku 
                           JOIN vendor v ON vs.VENDOR_ID = v.ID,
                           return_order_sku_reason rosr
      where ros.return_order_sku_reason_id=rosr.return_order_sku_reason_id
      and to_date(To_char(roh.created_date, 'YYYY-MON-DD'),'YYYY-MON-DD') >= To_date(:date1_bv, 'MM/DD/YYYY')
      and to_date(To_char(roh.created_date, 'YYYY-MON-DD'),'YYYY-MON-DD') <= To_date(:date2_bv, 'MM/DD/YYYY')
      and ro.return_order_type_id in (1,2,3)
      AND s.is_drop_ship = 1
                         
) a1
) a2
WHERE row_num = 1 
      ;
      

