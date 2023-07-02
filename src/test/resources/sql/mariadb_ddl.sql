-- willmores.willmores definition

CREATE TABLE `willmores` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `given_name` varchar(255) DEFAULT NULL,
  `middle_name` varchar(255) DEFAULT NULL,
  `surname` varchar(255) DEFAULT NULL,
  `suffix` varchar(255) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=31 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- willmores.willmores_fullname definition

CREATE TABLE `willmores_fullname` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `fullname` varchar(510) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
