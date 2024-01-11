SELECT project_id,

       name,

       segment1,

       creation_date,

       project_type,

       carrying_out_organization_id,

       project_status_code,

       description,

       start_date,

       completion_date,

       attribute_category,

       attribute1,

       attribute2,

       attribute3,

       attribute4,

       attribute5,

       attribute6,

       attribute7,

       attribute8,

       attribute9,

       attribute10,

       pm_product_code,

       pm_project_reference,

       location_id,

       long_name,

       revenue_accrual_method,

       invoice_method,

       meta_effective_start_datetime,

       meta_effective_end_datetime,

       CASE

         WHEN (ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY meta_effective_start_datetime)) = 1 THEN 'INSERT'

         WHEN LAG(meta_operation_code) OVER (ORDER BY project_id,meta_effective_start_datetime) = 'DELETE' THEN 'INSERT'

         ELSE meta_operation_code

       END AS meta_operation_code,

       meta_curr_rec_ind

FROM (SELECT grp,

             project_id,

             name,

             segment1,

             creation_date,

             project_type,

             carrying_out_organization_id,

             project_status_code,

             description,

             start_date,

             completion_date,

             attribute_category,

             attribute1,

             attribute2,

             attribute3,

             attribute4,

             attribute5,

             attribute6,

             attribute7,

             attribute8,

             attribute9,

             attribute10,

             pm_product_code,

             pm_project_reference,

             location_id,

             long_name,

             revenue_accrual_method,

             invoice_method,

             MIN(meta_effective_start_datetime) AS meta_effective_start_datetime,

             MAX(meta_effective_end_datetime) AS meta_effective_end_datetime,

             MIN(meta_operation_code) AS meta_operation_code,

             MAX(meta_curr_rec_ind) AS meta_curr_rec_ind

      FROM (SELECT g.*,

                   SUM(g.consec) OVER (ORDER BY project_id,meta_effective_start_datetime ROWS UNBOUNDED PRECEDING) AS grp

            FROM (SELECT s.*,

                         CASE

                           WHEN LAG(ranking) OVER (ORDER BY project_id,meta_effective_start_datetime) = ranking THEN NULL

                           ELSE 1

                         END AS consec

                  FROM (SELECT stg.stg_pa_projects_all.project_id,

                               stg.stg_pa_projects_all.name,

                               stg.stg_pa_projects_all.segment1,

                               stg.stg_pa_projects_all.creation_date,

                               stg.stg_pa_projects_all.project_type,

                               stg.stg_pa_projects_all.carrying_out_organization_id,

                               stg.stg_pa_projects_all.project_status_code,

                               stg.stg_pa_projects_all.description,

                               stg.stg_pa_projects_all.start_date,

                               stg.stg_pa_projects_all.completion_date,

                               stg.stg_pa_projects_all.attribute_category,

                               stg.stg_pa_projects_all.attribute1,

                               stg.stg_pa_projects_all.attribute2,

                               stg.stg_pa_projects_all.attribute3,

                               stg.stg_pa_projects_all.attribute4,

                               stg.stg_pa_projects_all.attribute5,

                               stg.stg_pa_projects_all.attribute6,

                               stg.stg_pa_projects_all.attribute7,

                               stg.stg_pa_projects_all.attribute8,

                               stg.stg_pa_projects_all.attribute9,

                               stg.stg_pa_projects_all.attribute10,

                               stg.stg_pa_projects_all.pm_product_code,

                               stg.stg_pa_projects_all.pm_project_reference,

                               stg.stg_pa_projects_all.location_id,

                               stg.stg_pa_projects_all.long_name,

                               stg.stg_pa_projects_all.revenue_accrual_method,

                               stg.stg_pa_projects_all.invoice_method,

                               stg.stg_pa_projects_all.meta_effective_start_datetime,

                               stg.stg_pa_projects_all.meta_effective_end_datetime,

                               CASE

                                 WHEN stg.stg_pa_projects_all.meta_operation_code = 'INSERT' THEN 'UPDATE'

                                 ELSE stg.stg_pa_projects_all.meta_operation_code

                               END AS meta_operation_code,

                               stg.stg_pa_projects_all.meta_curr_rec_ind,

                               DENSE_RANK() OVER (ORDER BY stg.stg_pa_projects_all.project_id,stg.stg_pa_projects_all.name,stg.stg_pa_projects_all.segment1,

                               stg.stg_pa_projects_all.creation_date,stg.stg_pa_projects_all.project_type,stg.stg_pa_projects_all.carrying_out_organization_id,

                               stg.stg_pa_projects_all.project_status_code,stg.stg_pa_projects_all.description,stg.stg_pa_projects_all.start_date,

                               stg.stg_pa_projects_all.completion_date,stg.stg_pa_projects_all.attribute_category,stg.stg_pa_projects_all.attribute1,stg.stg_pa_projects_all.attribute2,stg.stg_pa_projects_all.attribute3,stg.stg_pa_projects_all.attribute4,stg.stg_pa_projects_all.attribute5,stg.stg_pa_projects_all.attribute6,stg.stg_pa_projects_all.attribute7,stg.stg_pa_projects_all.attribute8,stg.stg_pa_projects_all.attribute9,stg.stg_pa_projects_all.attribute10,stg.stg_pa_projects_all.pm_product_code,stg.stg_pa_projects_all.pm_project_reference,stg.stg_pa_projects_all.location_id,stg.stg_pa_projects_all.long_name,stg.stg_pa_projects_all.revenue_accrual_method,stg.stg_pa_projects_all.invoice_method,

                               CASE WHEN stg.stg_pa_projects_all.meta_operation_code = 'INSERT' THEN 'UPDATE' ELSE stg.stg_pa_projects_all.meta_operation_code END) AS ranking

                        FROM stg.stg_pa_projects_all

                        WHERE stg.stg_pa_projects_all.project_id in (201475, 199104, 125072,206553,130876,60302,205554)

                        AND   stg.stg_pa_projects_all.meta_curr_rec_ind IN (1,0)) s) g)

      GROUP BY grp,

               project_id,

               name,

               segment1,

               creation_date,

               project_type,

               carrying_out_organization_id,

               project_status_code,

               description,

               start_date,

               completion_date,

               attribute_category,

               attribute1,

               attribute2,

               attribute3,

               attribute4,

               attribute5,

               attribute6,

               attribute7,

               attribute8,

               attribute9,

               attribute10,

               pm_product_code,

               pm_project_reference,

               location_id,

               long_name,

               revenue_accrual_method,

               invoice_method

      ORDER BY project_id,meta_effective_start_datetime)

ORDER BY project_id,meta_effective_start_datetime;