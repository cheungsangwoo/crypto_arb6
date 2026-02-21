-- --------------------------------------------------------
-- 호스트:                          127.0.0.1
-- 서버 버전:                        12.0.2-MariaDB - mariadb.org binary distribution
-- 서버 OS:                        Win64
-- HeidiSQL 버전:                  12.11.0.7065
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


-- crypto_arb3 데이터베이스 구조 내보내기
CREATE DATABASE IF NOT EXISTS `crypto_arb3` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_uca1400_ai_ci */;
USE `crypto_arb3`;

-- 테이블 crypto_arb3.exchange_rules 구조 내보내기
CREATE TABLE IF NOT EXISTS `exchange_rules` (
  `symbol` varchar(50) NOT NULL,
  `spot_exchange` varchar(20) NOT NULL,
  `binance_step_size` float DEFAULT NULL,
  `binance_tick_size` float DEFAULT NULL,
  `binance_min_qty` float DEFAULT NULL,
  `binance_min_notional` float DEFAULT NULL,
  `spot_step_size` float DEFAULT NULL,
  `spot_min_notional` float DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  PRIMARY KEY (`symbol`,`spot_exchange`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- 내보낼 데이터가 선택되어 있지 않습니다.

-- 테이블 crypto_arb3.market_metrics 구조 내보내기
CREATE TABLE IF NOT EXISTS `market_metrics` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `timestamp` datetime DEFAULT NULL,
  `exchange` varchar(20) DEFAULT NULL,
  `median_entry_premium` float DEFAULT NULL,
  `median_exit_premium` float DEFAULT NULL,
  `opportunity_count` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- 내보낼 데이터가 선택되어 있지 않습니다.

-- 테이블 crypto_arb3.portfolio_snapshots 구조 내보내기
CREATE TABLE IF NOT EXISTS `portfolio_snapshots` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `timestamp` datetime DEFAULT NULL,
  `total_usdt_value` float DEFAULT NULL,
  `total_krw_value` float DEFAULT NULL,
  `binance_usdt_free` float DEFAULT NULL,
  `binance_unrealized_pnl` float DEFAULT NULL,
  `spot_krw_free` float DEFAULT NULL,
  `fx_rate` float DEFAULT NULL,
  `details` text DEFAULT NULL,
  `inventory_val_usdt` float DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=128 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- 내보낼 데이터가 선택되어 있지 않습니다.

-- 테이블 crypto_arb3.positions 구조 내보내기
CREATE TABLE IF NOT EXISTS `positions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `symbol` varchar(20) NOT NULL,
  `exchange` varchar(20) NOT NULL,
  `status` varchar(20) DEFAULT NULL,
  `current_spot_qty` float DEFAULT NULL,
  `current_hedge_qty` float DEFAULT NULL,
  `entry_time` datetime DEFAULT NULL,
  `entry_spot_qty` float DEFAULT NULL,
  `entry_spot_price` float DEFAULT NULL,
  `entry_spot_amount_krw` float DEFAULT NULL,
  `entry_hedge_qty` float DEFAULT NULL,
  `entry_hedge_price` float DEFAULT NULL,
  `entry_hedge_amount_usdt` float DEFAULT NULL,
  `exit_time` datetime DEFAULT NULL,
  `exit_spot_qty` float DEFAULT NULL,
  `exit_spot_price` float DEFAULT NULL,
  `exit_spot_amount_krw` float DEFAULT NULL,
  `exit_hedge_qty` float DEFAULT NULL,
  `exit_hedge_price` float DEFAULT NULL,
  `exit_hedge_amount_usdt` float DEFAULT NULL,
  `gross_spot_pnl_krw` float DEFAULT NULL,
  `gross_hedge_pnl_usdt` float DEFAULT NULL,
  `total_fees_usdt` float DEFAULT NULL,
  `net_pnl_usdt` float DEFAULT NULL,
  `entry_usdt_rate` float DEFAULT NULL,
  `exit_usdt_rate` float DEFAULT NULL,
  `config_entry_threshold` float DEFAULT NULL,
  `config_exit_threshold` float DEFAULT NULL,
  `calc_entry_premium` float DEFAULT NULL,
  `calc_exit_premium` float DEFAULT NULL,
  `entry_spot_order_id` varchar(100) DEFAULT NULL,
  `entry_hedge_order_id` varchar(100) DEFAULT NULL,
  `exit_spot_order_id` varchar(100) DEFAULT NULL,
  `exit_hedge_order_id` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=68 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- 내보낼 데이터가 선택되어 있지 않습니다.

-- 테이블 crypto_arb3.strategy_collector 구조 내보내기
CREATE TABLE IF NOT EXISTS `strategy_collector` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `timestamp` datetime DEFAULT current_timestamp(),
  `symbol` varchar(20) DEFAULT NULL,
  `spot_exchange` varchar(20) DEFAULT NULL,
  `spot_bid_price` float DEFAULT NULL,
  `spot_ask_price` float DEFAULT NULL,
  `spot_bid_size` float DEFAULT NULL,
  `binance_bid_price` float DEFAULT NULL,
  `binance_ask_price` float DEFAULT NULL,
  `binance_mark_price` float DEFAULT NULL,
  `funding_rate` float DEFAULT NULL,
  `next_funding_time` datetime DEFAULT NULL,
  `annualized_funding_pct` float DEFAULT NULL,
  `implied_fx` float DEFAULT NULL,
  `kimchi_premium_pct` float DEFAULT NULL,
  `entry_premium_pct` float DEFAULT NULL,
  `exit_premium_pct` float DEFAULT NULL,
  `ref_usdt_ask` float DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_strategy_collector_symbol` (`symbol`),
  KEY `ix_strategy_collector_id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- 내보낼 데이터가 선택되어 있지 않습니다.

/*!40103 SET TIME_ZONE=IFNULL(@OLD_TIME_ZONE, 'system') */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;
