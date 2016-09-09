(* Asynchronous IO library.
 *
 * For each blocking action, if the action can be performed immediately, then it
 * is. Otherwise, the thread performing the blocking task is suspended and
 * automatically wakes up when the action completes. The suspend/resume is
 * transparent to the programmer.
 *)

type file_descr = Unix.file_descr
type sockaddr = Unix.sockaddr
type msg_flag = Unix.msg_flag

type 'a promise

val async  : ('a -> 'b) -> 'a -> 'b promise
val await  : 'a promise -> 'a 
(** Raises exception [e] if the promise raises [e]. *)
val yield  : unit -> unit

val accept : file_descr -> file_descr * sockaddr
val recv   : file_descr -> bytes -> int -> int -> msg_flag list -> int
val send   : file_descr -> bytes -> int -> int -> msg_flag list -> int
val sleep  : float -> unit

val run : (unit -> unit) -> unit
