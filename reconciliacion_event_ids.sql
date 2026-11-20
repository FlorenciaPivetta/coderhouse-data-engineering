WITH kinesis(event_id, event_type, amount) AS (
    SELECT *
    FROM (VALUES
('79ef7df1-f76f-4e59-925c-80338c0b1a3e', 'view', 442.53),
('08c7adc8-cd4b-47f3-997a-a360d3a8f802', 'click', 311.58),
('577f322a-6fa7-4bf0-86ff-0cc798499fbc', 'view', 406.76),
('2c023b5a-2b1e-412e-abe6-ed657bf47826', 'view', 77.99),
('dd22a651-84ea-4670-ba3c-d63c64dc43ae', 'click', 205.73),
('347b424a-1338-4579-bae6-1cf2fdc66dcb', 'purchase', 451.01),
('5c71e757-b10d-4b84-8c8c-85cb16093c81', 'refund', 475.23),
('6342e765-5d9c-412c-86e5-91e21500cbc7', 'refund', 307.20),
('6e46b4cd-d9cd-45de-954d-dc0ca78c585b', 'click', 472.72),
('01e65e34-835c-4ba1-8895-3acfe2326c2f', 'view', 179.17),
('548539cd-bd0e-414e-b413-ca15264cb432', 'view', 10.22),
('687e1f4e-8c0f-4cf0-8f14-8bf9ecc204ed', 'click', 249.14),
('e102f1ae-37dd-4f09-b17e-276ced56a448', 'purchase', 98.00),
('4b578625-05b5-4efd-9aab-b67dd55ad11c', 'click', 121.41),
('1b1d98bc-66e3-4119-9a61-fda71a3dd166', 'purchase', 343.22),
('99a238eb-297e-47ca-84a9-5b6e28847a2f', 'view', 397.74),
('beab71a9-dd2a-4f6a-8acb-f6ce8aa64450', 'click', 307.94),
('c1a66ca1-2d45-41aa-8aa5-8cd771911e1b', 'purchase', 137.10),
('4777586c-7c2c-4610-b1db-e8b0545b0a2a', 'view', 264.62),
('52dd0ccd-ada6-40bb-827e-2eea5af3d218', 'refund', 281.92),
('e21d170e-5f06-44fc-b4bb-4c0c425c68ea', 'view', 197.27),
('c74f4d7d-7470-460c-af6a-353accd593ec', 'view', 202.58),
('687cab5c-0f8e-477d-abb2-6a600bde30ad', 'purchase', 102.75),
('8a912ef5-8cd9-486e-9dd5-59d849316a8a', 'refund', 403.69),
('d4981912-b5bb-46fb-98e7-dc26795f37f1', 'click', 347.07),
('c3a8deba-32ec-41b1-b4e0-e79d26c33bc4', 'refund', 100.82),
('fc373357-4697-434b-b2f9-62f52c6a6c60', 'purchase', 397.68),
('8f33c37e-de37-4b9f-bfec-8c48b90d934a', 'view', 321.32),
('6666fcfe-d732-448d-9337-0898be378757', 'purchase', 23.12),
('86be33fa-92fa-4f1c-b818-2b5bb9aad22f', 'view', 258.96),
('0f687e43-abe6-4a0b-a96a-9d5fe23c4271', 'purchase', 441.36),
('2a75bd96-d02e-4f53-ba72-2a8be8bd6289', 'purchase', 229.68),
('23b52747-e5c1-4914-afab-31b0216b3b2f', 'refund', 107.11),
('f6d47a87-90be-41b4-b468-65bd6e71b25b', 'purchase', 485.69),
('fdbf0baf-8564-405d-9f5a-41f08fe8800a', 'purchase', 10.86),
('9fee5b54-c09b-4b54-ae6a-eb9f68fb7fcc', 'refund', 195.86),
('3c97f675-ba3f-4e65-a30e-babb1ec36556', 'view', 184.66),
('96cfa132-257d-4740-93b6-5409b16b5e76', 'view', 343.21),
('463cce6f-c28b-4f8b-9989-fd0aa20b17de', 'click', 22.63),
('90ad6dc7-0e40-4d1b-9718-fba11ab323a7', 'refund', 261.59),
('038f5978-8d69-4c5f-9dcf-9a18701a7147', 'purchase', 243.53),
('2e0c5f41-3c18-4472-95fd-382159517eb8', 'view', 214.15),
('5548c9b3-63ed-4af0-b779-6f78c2d30826', 'click', 96.72),
('de62b610-cc17-4b11-8adc-1030d63cee7b', 'refund', 13.84),
('bd9c4b11-dff8-45c1-8e6e-139c84ddd6b1', 'click', 367.47),
('ef2acae5-6c84-4f2b-be30-3356741d1787', 'click', 20.20),
('e06aab55-6f87-479f-8e1e-8d569ce8d1e0', 'purchase', 379.06),
('1ded68d8-8c6d-48dd-b13e-78782016039f', 'purchase', 238.67),
('02503c90-772a-4b8c-8bd1-302b5fdb0d90', 'refund', 418.03),
('0200146b-825b-4b35-9a3b-d9811b4f3cf6', 'click', 306.23),
('13be0976-6636-4d7d-8960-752e902eab0b', 'click', 407.36),
('b1448853-0316-46d7-9c16-effc9c79a3d4', 'purchase', 369.33),
('82c7dc58-1a67-4c9c-8f4f-a5a1a309b2d1', 'view', 187.12),
('30fd56f5-47e9-407c-b9bd-d1c56e07b350', 'purchase', 53.43),
('1e80f18f-1828-4a98-8b22-4cc6879b02ac', 'refund', 376.38),
('2a80f85d-9b26-49df-8b12-43ec82400372', 'view', 14.99),
('4ab6be4d-b8e8-4ec2-bdc0-557149068c91', 'click', 448.50),
('8356e39b-96a9-4dd4-97af-d65269b0a043', 'view', 381.86),
('5b99f976-d2ee-447b-bbcb-3bb49980c2ac', 'refund', 106.60),
('304d23d3-a6e3-45b3-8f38-d3403104ef05', 'refund', 317.29),
('72442000-fea2-429e-b2f7-56edb1620fc4', 'refund', 148.46),
('d4050d4b-d6f3-4d75-ade6-fbf6f42833ab', 'refund', 62.26),
('4eb9b8b4-f488-4fb8-874b-286a109531b5', 'click', 209.85),
('e2947b77-6ad0-4f66-b23f-e24123519a52', 'view', 240.25),
('95054a2e-8898-48f9-b392-ffcc9b16299e', 'view', 347.81),
('97673fda-e344-4838-b2ca-066da3f198a7', 'click', 412.82),
('9ec7e5f2-3a7a-4da4-9a05-746cbc6e3310', 'view', 391.20),
('782ce529-89c0-4afd-b8a1-87a0ed1857a1', 'click', 89.89),
('ab31229b-a9e6-4d06-842e-06c46cdb2e53', 'purchase', 110.14),
('9a2dfd3a-0082-4f22-9860-f559e452d0d2', 'refund', 153.28),
('67b2154b-086c-404f-8589-17c1558cc6a0', 'purchase', 432.49),
('24e38b20-adb8-4212-9547-53c34272efac', 'purchase', 274.36),
('d63b8399-73b4-45c1-9c03-71c45762d325', 'view', 30.34),
('21a4c01f-5c2a-4a5a-9b39-ba0419f21b99', 'refund', 461.02),
('e30831ba-8bd0-4595-87d0-04af59bcec10', 'view', 31.20),
('9d14c53f-e2b1-4fe1-a878-46e1a51ec1d5', 'purchase', 235.12),
('24c642cb-ef57-4fd2-8644-2ce5de346252', 'refund', 389.89),
('2d52f463-8315-406f-8c11-3cef398a8232', 'purchase', 489.84),
('2dc46cb3-ee14-427e-ac61-fc691eb13897', 'refund', 346.04),
('38abde9f-215a-4fbe-bdb8-ca0831010e83', 'click', 163.67),
('ef388b27-3c0e-428e-b91d-0201d0cbb8df', 'view', 410.37),
('be60e49b-6533-418a-932c-e3451ef47764', 'click', 256.32),
('21463e5f-9574-4cec-adc9-9ca137c906ba', 'view', 434.67),
('15abcded-8576-4c98-a143-d1f8d5892326', 'click', 105.66),
('52ed5cea-9c4c-4bc1-a502-a95fc02ebaf8', 'click', 47.63),
('a2c681e2-90da-4e1c-a304-08d357b26434', 'view', 157.57),
('ea0ba2f7-935e-4377-8989-5497cfd556ae', 'view', 66.99),
('ab213729-452a-45a2-8afa-f626e96b6673', 'view', 400.47),
('c4bb0b64-2bb8-4151-b113-4ca55ed9956e', 'purchase', 481.26),
('cbd6c4f2-c910-4794-a2fe-777d059c82a9', 'click', 303.95),
('e528e704-6ae2-42a3-a8e6-e81f05af2c3e', 'click', 298.89),
('f3635a0c-c0bb-40ff-8766-0c884cb1591b', 'click', 244.31),
('e91159ea-d8b9-490e-b19e-4de779926d2c', 'view', 453.60),
('1f450267-2544-4bf8-bc70-7cd4231d17b6', 'click', 80.30),
('a65c8525-0ece-4387-8c1e-0e68d3cc4e03', 'purchase', 54.29),
('988f2d3a-f4b1-4194-9074-22f157d094c0', 'click', 341.32),
('2e684ca9-aeda-4041-beb0-4db9d28f532b', 'click', 421.99),
('7766695d-f6c3-45a1-8cd0-ee94b2e748b1', 'click', 245.23),
('b7360384-8590-4c15-b711-187114edaf65', 'refund', 33.18),
('c32df7a7-00f6-46a0-9341-044a90fa897b', 'view', 164.90)
    ) AS v(event_id, event_type, amount)
),
redshift AS (
    SELECT event_id, event_type, payload_value
    FROM analytics.vw_kinesis_iceberg
    WHERE event_timestamp::VARCHAR >= '2026-11-18T03:08:00'
      AND event_timestamp::VARCHAR <  '2026-11-18T03:11:00'
)
SELECT
    COALESCE(k.event_id, r.event_id) AS event_id,
    CASE
        WHEN k.event_id IS NULL THEN 'SOLO_EN_REDSHIFT'
        WHEN r.event_id IS NULL THEN 'SOLO_EN_KINESIS'
        WHEN k.event_type <> r.event_type THEN 'TIPO_DIFERENTE'
        WHEN ABS(k.amount - r.payload_value) > 0.001 THEN 'IMPORTE_DIFERENTE'
        ELSE 'OK'
    END AS resultado
FROM kinesis k
FULL OUTER JOIN redshift r
    ON k.event_id = r.event_id
WHERE k.event_id IS NULL
   OR r.event_id IS NULL
   OR k.event_type <> r.event_type
   OR ABS(k.amount - r.payload_value) > 0.001
ORDER BY resultado, event_id;
