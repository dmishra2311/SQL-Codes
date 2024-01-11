select

         wbshierarchypathid,

         wbsid,

         wbsinternalid,

         wbsname,

         parentpathid,

         communitiesofpracticename,

         chargeableflag,

         originalbudgetstartdate,

         originalbudgetenddate,

         currentbudgetstartdate,

         currentbudgetenddate,

         currentforecaststartdate,

         currentforecastenddate,

         p6actualstartdate,

         p6actualenddate,

         meta_effective_start_datetime,

         meta_effective_end_datetime,

         case when (row_number() over (partition by wbsinternalid order by meta_effective_start_datetime))=1 then 'INSERT'

         when lag (meta_operation_code) over (order by wbsinternalid,meta_effective_start_datetime) = 'DELETE' then 'INSERT'

         else meta_operation_code end as meta_operation_code,

         meta_curr_rec_ind

        from

(

select grp,wbshierarchypathid,wbsid,wbsinternalid,wbsname,parentpathid,

         communitiesofpracticename,

         chargeableflag,

         originalbudgetstartdate,

         originalbudgetenddate,

         currentbudgetstartdate,

         currentbudgetenddate,

         currentforecaststartdate,

         currentforecastenddate,

         p6actualstartdate,

         p6actualenddate,min(meta_effective_start_datetime) as meta_effective_start_datetime,max(meta_effective_end_datetime) as meta_effective_end_datetime,

         MIN(meta_operation_code) AS meta_operation_code,

       MAX(meta_curr_rec_ind) AS meta_curr_rec_ind from

(select g.*,sum(g.consec) over (order by wbsinternalid,meta_effective_start_datetime rows unbounded preceding) as grp from

(select s.*, case when lag (ranking) over (order by wbsinternalid,meta_effective_start_datetime) = ranking then null else 1 end as consec from

(SELECT stg.stg_epc_project_wbs1.wbshierarchypathid,stg.stg_epc_project_wbs1.wbsid,stg.stg_epc_project_wbs1.wbsinternalid,

                   stg.stg_epc_project_wbs1.wbsname,stg.stg_epc_project_wbs1.parentpathid,stg.stg_epc_project_wbs1.communitiesofpracticename,

                   stg.stg_epc_project_wbs1.chargeableflag,stg.stg_epc_project_wbs1.originalbudgetstartdate,stg.stg_epc_project_wbs1.originalbudgetenddate,

                   stg.stg_epc_project_wbs1.currentbudgetstartdate,stg.stg_epc_project_wbs1.currentbudgetenddate,stg.stg_epc_project_wbs1.currentforecaststartdate,

                   stg.stg_epc_project_wbs1.currentforecastenddate,stg.stg_epc_project_wbs1.p6actualstartdate,stg.stg_epc_project_wbs1.p6actualenddate,

                   stg.stg_epc_project_wbs1.meta_effective_start_datetime,stg.stg_epc_project_wbs1.meta_effective_end_datetime,

                   case when stg.stg_epc_project_wbs1.meta_operation_code='INSERT' then 'UPDATE' else stg.stg_epc_project_wbs1.meta_operation_code end as

                   meta_operation_code,

                   stg.stg_epc_project_wbs1.meta_curr_rec_ind,

                   DENSE_RANK() OVER (ORDER BY stg.stg_epc_project_wbs1.wbsinternalid,stg.stg_epc_project_wbs1.wbshierarchypathid,stg.stg_epc_project_wbs1.wbsid,

                   stg.stg_epc_project_wbs1.wbsname,stg.stg_epc_project_wbs1.parentpathid,stg.stg_epc_project_wbs1.communitiesofpracticename,

                   stg.stg_epc_project_wbs1.chargeableflag,stg.stg_epc_project_wbs1.originalbudgetstartdate,stg.stg_epc_project_wbs1.originalbudgetenddate,

                   stg.stg_epc_project_wbs1.currentbudgetstartdate,stg.stg_epc_project_wbs1.currentbudgetenddate,stg.stg_epc_project_wbs1.currentforecaststartdate,

                   stg.stg_epc_project_wbs1.currentforecastenddate,stg.stg_epc_project_wbs1.p6actualstartdate,stg.stg_epc_project_wbs1.p6actualenddate) AS ranking

            FROM stg.stg_epc_project_wbs1

            WHERE stg.stg_epc_project_wbs1.wbsinternalid = 368749

            AND   stg.stg_epc_project_wbs1.meta_curr_rec_ind IN (1,0)

           ) s ) g )

            group by grp,wbshierarchypathid,

         wbsid,wbsinternalid,

         wbsname,

         parentpathid,

         communitiesofpracticename,

         chargeableflag,

         originalbudgetstartdate,

         originalbudgetenddate,

         currentbudgetstartdate,

         currentbudgetenddate,

         currentforecaststartdate,

         currentforecastenddate,

         p6actualstartdate,

         p6actualenddate,meta_operation_code order by meta_effective_start_datetime

         )

order by meta_effective_start_datetime;