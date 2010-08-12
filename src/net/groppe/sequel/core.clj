(ns net.groppe.sequel.core
  (:require [clojure.contrib.str-utils :as str-utils]))

(defn- sqlize
  [x]
  (cond
   (keyword? x) (str-utils/re-sub #"\/" "." (str-utils/re-sub #"^:" "" (str x)))
   (string? x) (str "'" x "'")
   :else x))

(defn- sql-exp
  [op p1 p2]
  (str (sqlize p1) " " op " " (sqlize p2)))

(defmacro filter
  [pred query]
  `(let [~'and (fn[& xs#] (apply str (interpose " AND " xs#)))
	 ~'or (fn[& xs#] (apply str (interpose " OR " xs#)))
	 ~'= (fn[x# y#] (sql-exp "=" x# y#))
	 ~'> (fn[x# y#] (sql-exp ">" x# y#))
	 ~'< (fn[x# y#] (sql-exp "<" x# y#))
	 ~'like (fn[x# y#] (sql-exp "like" x# y#))
	 ~'in (fn[x# xs#]
		(str (sqlize x#) " IN (" (apply str (interpose ", " xs#)) ")"))]
     (apply str ~query " where " ~pred)))

(defn collect*
  [table & xs]
  (if (vector? (last xs))
    (let [tbls (interpose ", " (map #(sqlize %) (conj (butlast xs) table)))
	  cols (str-utils/str-join ", " (map #(sqlize %) (last xs)))]
      (apply str "select " cols " from " tbls))
    (let [tbls (interpose ", " (map #(sqlize %) (conj xs table)))]
      (apply str "select * from " tbls))))

(defmacro collect
  [table & xs]
  `(let [~'as (fn[name# col#] (str (sqlize col#) " AS " (sqlize name#)))]
     (collect* ~table ~@xs)))
