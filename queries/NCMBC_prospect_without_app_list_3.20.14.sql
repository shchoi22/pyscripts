
with 
 pw_leases as 
   (select pw_lease.*
     ,replace(replace(replace(replace(pw_lease.primarycontacthomephone,' ',''),'-',''),'(',''),')','') as home_phone
     ,replace(replace(replace(replace(pw_lease.primarycontactmobile,' ',''),'-',''),'(',''),')','') as mobile_phone
     ,replace(replace(replace(replace(pw_lease.primarycontactworkphone,' ',''),'-',''),'(',''),')','') as work_phone
     ,(case when length(replace(replace(replace(replace(pw_lease.primarycontacthomephone,' ',''),'-',''),'(',''),')','')) >=10 
                 then replace(replace(replace(replace(pw_lease.primarycontacthomephone,' ',''),'-',''),'(',''),')','') else '' end) || 
      (case when length(replace(replace(replace(replace(pw_lease.primarycontactmobile,' ',''),'-',''),'(',''),')','')) >=10
                 then replace(replace(replace(replace(pw_lease.primarycontactmobile,' ',''),'-',''),'(',''),')','') else '' end) ||
      (case when length(replace(replace(replace(replace(pw_lease.primarycontactworkphone,' ',''),'-',''),'(',''),')','')) >= 10
                 then replace(replace(replace(replace(pw_lease.primarycontactworkphone,' ',''),'-',''),'(',''),')','') else '' end) as combined_phone
    from pw_lease)

SELECT
  prospect_data.first_name
  ,prospect_data.last_name
  ,case when secondary.first_name is null then 'N/A' else secondary.first_name end as co_applicant_first_name
  ,case when secondary.last_name is null then 'N/A' else secondary.last_name end as co_applicant_last_name
  ,prospect_data.mobile_phone
  ,prospect_data.home_phone
  ,prospect_data.work_phone
  ,prospect_data.email
  
FROM 
 --(select * from pcore_prospect_data where pcore_prospect_data.primary_or_secondary_applicant <> 'Secondary') as prospect_data

  (select prospect_data.*
          ,dups.id as dups_id
          ,dups.combined as dups_combined
   from (select * from pcore_prospect_data where pcore_prospect_data.primary_or_secondary_applicant <> 'Secondary') as prospect_data
   left outer join pcore_prospect_data as dups on prospect_data.id <> dups.id
                                         and dups.application_processed_on !=''
                                         and 
                                         ((length(prospect_data.combined) >= length(dups.combined)

                                         and ((length(dups.mobile_phone) >=10 and prospect_data.combined like '%'||dups.mobile_phone||'%')
                                             or (length(dups.home_phone) >=10 and prospect_data.combined like '%'||dups.home_phone||'%')
                                             or (length(dups.work_phone) >=10 and prospect_data.combined like '%'||dups.work_phone||'%')))
                                         or((length(prospect_data.combined) < length(dups.combined)
                                         and ((length(prospect_data.mobile_phone) >=10 and dups.combined like '%'||prospect_data.mobile_phone||'%')
                                             or (length(prospect_data.home_phone) >=10 and dups.combined like '%'||prospect_data.home_phone||'%')
                                             or (length(prospect_data.work_phone) >=10 and dups.combined like '%'||prospect_data.work_phone||'%')))))
                                         and prospect_data.first_name = dups.first_name
					 and prospect_data.last_name = dups.last_name
                                         /*
                                         and
                                        (((length(prospect_data.mobile_phone) >=10 and
				           dups.combined like '%'||prospect_data.mobile_phone||'%') or
                                            (length(dups.mobile_phone) >=10 and
				            prospect_data.combined like '%'||dups.mobile_phone||'%')) or

					((length(prospect_data.home_phone) >=10 and
					dups.combined like '%'||prospect_data.home_phone||'%') or
					(length(dups.home_phone) >=10 and
					prospect_data.combined like '%'||dups.home_phone||'%')) or

					((length(prospect_data.work_phone) >=10 and 
					dups.combined like '%'||prospect_data.work_phone||'%') or
					(length(dups.work_phone) >=10 and 
					prospect_data.combined like '%'||dups.work_phone||'%')) 

					and prospect_data.first_name = dups.first_name 
					and prospect_data.last_name = dups.last_name)
                                        */
    where dups.id is null) as prospect_data
 
  left outer join (select * 
                   from pcore_prospect_data
                   where pcore_prospect_data.primary_or_secondary_applicant = 'Secondary') as secondary on prospect_data.application_id = secondary.application_id

  left outer join pw_building on prospect_data.desired_building_name = pw_building.buildingabbreviation

  left outer join pw_leases on --Primary
                  (((length(prospect_data.mobile_phone) >=10 and
                  pw_leases.combined_phone like '%'||prospect_data.mobile_phone||'%') or
                   (length(prospect_data.home_phone) >=10 and
                   pw_leases.combined_phone like '%'||prospect_data.home_phone||'%') or
                   (length(prospect_data.work_phone) >=10 and 
                   pw_leases.combined_phone like '%'||prospect_data.work_phone||'%')) and
                  prospect_data.first_name = lower(pw_leases.primarycontactfirstname) and
                  prospect_data.last_name = lower(pw_leases.primarycontactlastname))
                  and cast(pw_leases.createdtime as date) >= cast(prospect_data.created as date) 
                  --or cast(pw_leases.createdtime as date) + interval '15 days' <= cast(prospect_data.created as date) )
                 /* or (((length(secondary.mobile_phone) >=10 or length(secondary.home_phone) >=10 or length(secondary.work_phone) >=10)
                   and pw_leases.combined_phone ~~* any(array['%'||secondary.mobile_phone||'%','%'||secondary.work_phone||'%','%'||secondary.home_phone||'%']))
                  and secondary.first_name = lower(pw_leases.primarycontactfirstname)
                  and secondary.last_name = lower(pw_leases.primarycontactlastname)
                  and cast(pw_leases.createdtime as date) >= cast(secondary.created as date))
*/
  left outer join pw_leases as pw_leases_sec on --Secondary
                  (((length(secondary.mobile_phone) >=10 and
                  pw_leases_sec.combined_phone like '%'||secondary.mobile_phone||'%') or
                   (length(secondary.home_phone) >=10 and
                   pw_leases_sec.combined_phone like '%'||secondary.home_phone||'%') or
                   (length(secondary.work_phone) >=10 and
                   pw_leases_sec.combined_phone like '%'||secondary.work_phone||'%')) and
                  secondary.first_name = lower(pw_leases_sec.primarycontactfirstname) and
                  secondary.last_name = lower(pw_leases_sec.primarycontactlastname))
                  and cast(pw_leases_sec.createdtime as date) >= cast(secondary.created as date)
                  --or cast(pw_leases_sec.createdtime as date) + interval '15 days' <= cast(secondary.created as date))

where 
 prospect_data.first_name != ''
 and prospect_data.last_name !=''
 and prospect_data.created >= '2013-10-01'
 and prospect_data.sub_lead_provider = 'NCMBC'
 and prospect_data.application_processed_on =''
 and pw_leases.leasename is null

 order by
 prospect_data.first_name
 ,prospect_data.last_name
 