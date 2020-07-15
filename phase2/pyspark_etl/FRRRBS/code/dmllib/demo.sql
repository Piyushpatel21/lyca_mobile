SELECT p.imsi ,p.roam_flag ,p.imsi_prefix_find ,q.roaming_partner_name
FROM ( SELECT x.imsi ,x.roam_flag ,isnull(x.imsi_prefix, '-1') imsi_prefix_find
        FROM ( SELECT a.imsi ,a.roam_flag ,b.imsi_prefix FROM uk_rrbs_dm.fact_rrbs_voice A
        LEFT JOIN ref_rrbs_roaming_partner_imsi B
        ON A.imsi LIKE CONCAT ( b.imsi_prefix ,'%' ) ) x
    ) p
LEFT JOIN ref_rrbs_roaming_partner_imsi q
ON q.is_valid = 1
AND p.roam_flag = q.roam_flag
AND p.imsi_prefix_find LIKE CONCAT ( q.imsi_prefix ,'%' ) ;
