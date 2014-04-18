--explain
with customers as 
(select
   trim(lower(replace(replace(customers.first_name,';',''),'/t',''))) as first_name_clean
  ,trim(lower(replace(replace(customers.last_name,';',''),'/t',''))) as last_name_clean
  ,replace(replace(replace(replace(customers.home_phone,' ',''),'-',''),'(',''),')','') as home_phone_clean
  ,replace(replace(replace(replace(customers.mobile_phone,' ',''),'-',''),'(',''),')','') as mobile_phone_clean
  ,replace(replace(replace(replace(customers.work_phone,' ',''),'-',''),'(',''),')','') as work_phone_clean
  ,(case when mobile_phone is null then '' else mobile_phone end)||(case when work_phone is null then '' else work_phone end)||(case when home_phone is null then '' else home_phone end) as combined
  ,customers.*
 from customers
 where 
  lower(customers.first_name) not like 'urist%'
  and lower(customers.first_name) not like '%test%'
  and lower(customers.last_name) not like '%test%'
  and customers.first_name !=''
  and customers.last_name !=''
  and customers.first_name is not null
  and customers.last_name is not null),

showings_clean as (
select
  --showings.id as showing_id
   customers.first_name_clean
  ,customers.last_name_clean
  ,customers.combined
  ,showings.start_time
  ,showings.unit_id
  ,max(showings.id) as showing_id
  ,min(last_application.application_id) as application_id
  --,string_agg(distinct last_application.applicant_type,'') as applicant_type

from
  public.showings
  left outer join prospects on showings.prospect_id = prospects.id
  left outer join customers on customers.id = prospects.customer_id
  left outer join applicants on applicants.customer_id = customers.id
  
--Application/Applicant Data ----------------------------------------------------------------------------------------------------------------------------

left outer join (select 
                  customers.first_name_clean
                 ,customers.last_name_clean
                 ,customers.combined
                 ,customers.mobile_phone
                 ,customers.work_phone
                 ,customers.home_phone
                 ,case when applications_cosigners.applicant_id is not null then applications_cosigners.application_id
                       when application_details.id is not null then application_details.id
                       else null end as application_id
                 ,applicants.applicant_type
                 ,applicants.id as applicant_id
                 
                 from applicants 
                 left outer join customers on applicants.customer_id = customers.id

                 left outer join (select applicant_id -- first application
		 ,min(application_id) as application_id
		 from applicants_applications
		 group by applicant_id) as last_application on last_application.applicant_id = applicants.id

                 left outer join applications as application_details on last_application.application_id = application_details.id

                 left outer join applications_cosigners on applications_cosigners.applicant_id = applicants.id -- cosigner
		 
                 where
                   last_application.application_id is not null
                   or applications_cosigners.applicant_id is not null) as last_application 
  on (((length(customers.combined) >= length(last_application.combined)
                     and ((length(last_application.mobile_phone) >=10 and customers.combined like '%'||last_application.mobile_phone||'%')
                      or (length(last_application.home_phone) >=10 and customers.combined like '%'||last_application.home_phone||'%')
                      or (length(last_application.work_phone) >=10 and customers.combined like '%'||last_application.work_phone||'%')))
                   or((length(customers.combined) < length(last_application.combined)
                    and ((length(customers.mobile_phone) >=10 and last_application.combined like '%'||customers.mobile_phone||'%')
                     or (length(customers.home_phone) >=10 and last_application.combined like '%'||customers.home_phone||'%')
                     or (length(customers.work_phone) >=10 and last_application.combined like '%'||customers.work_phone||'%')))))
                and customers.first_name_clean = last_application.first_name_clean
		 and customers.last_name_clean = last_application.last_name_clean)
		 --or last_application.applicant_id = applicants.id

where
  customers.id is not null
  --and showings.created_at > '2013-09-28'

group by
   customers.first_name_clean
  ,customers.last_name_clean
  ,customers.combined
  ,showings.start_time
  ,showings.unit_id)

select
   customers.id as customer_id
  ,showings_clean.application_id
  ,showings.created_at as showing_set_on
  ,applications.created_at as first_app_submitted_at
  ,customers.first_name
  ,customers.last_name
  ,customers.mobile_phone
  ,customers.home_phone
  ,customers.work_phone
  ,customers.combined
  ,customers.email
  ,customers.pw_id
  --,activities.user as showing_set_by
  ,showings.start_time as showing_date
  ,showings.showable_state as showing_status
  ,showings.name as leasing_agent
  ,buildings.name as building_name
  ,units.name as unit_name
  --,update_activities.user as cancelled_rescheduled_by
  ,showings.showable_canceled_by
  ,showings.showable_canceled_reason
  ,case when showings_clean.application_id is null then 'no app'
             when applications.app_state is null then 'Paper'
             else 'kiosk' end as applicant_type
  ,case when (showings_clean.application_id is not null and showings.created_at < applications.created_at + interval '60 days') 
             --or (applicants.id is not null and applicants.ssn_itin is not null and showings.created_at < applicants.created_at + interval '60 days') 
             then 'yes' else 'no' end as app_in_before_showing
  ,case when showings_clean.application_id is not null and showings.showable_state = 'showing_set' and 
             applications.created_at > showings.start_time - interval '1 hour' and applications.created_at < showings.start_time + interval '1 hour' then 'yes' else 'no' end as received_app_at_showing
  ,case when showings_clean.application_id is not null and showings.showable_state = 'showing_set' and 
             applications.created_at > showings.start_time + interval '1 hour' and applications.created_at < showings.start_time + interval '8 hour' then 'yes' else 'no' end as received_app_within_8_hrs
  ,case when last_showing.id is not null then 'yes' else 'no' end as last_showing
  ,case when last_showing.id is not null and showings.showable_state = 'showing_set' and lease_signings.count_of > 0 then 'yes' else 'no' end as last_showing_to_convert

from showings_clean
left outer join showings on showings_clean.showing_id = showings.id
left outer join prospects on showings.prospect_id = prospects.id
left outer join customers on prospects.customer_id = customers.id
left outer join units on showings.unit_id = units.id
left outer join buildings on buildings.id = units.building_id
left outer join applications on showings_clean.application_id = applications.id
left outer join applicants on customers.id = applicants.customer_id
left outer join (select max(showings.id) as id 
                   from showings 
                   where showings.showable_state ='showing_set'
                   group by showings.prospect_id) as last_showing on last_showing.id = showings.id
left outer join (select lease_signings.prospect_id, count(*) as count_of from lease_signings
                   where lease_signings.showable_state = 'lease_signing_set'
                   group by lease_signings.prospect_id) as lease_signings on lease_signings.prospect_id = showings.prospect_id
left outer join (select customer_id -- occupants
		 ,min(application_id) as application_id
		 from applications_occupants
		 group by customer_id) as occupant_application on occupant_application.customer_id = customers.id

where showings.name !=''
and showings.start_time is not null
--and customers.id = '161030'
 --and cast(customers.created_at as date) >= '2013-09-28'
--and showings_clean.application_id is null
--and applicants.id is not null
--and applicants.ssn_itin is not null
 and occupant_application.customer_id is null
order by
   showing_set_on 