(* Asynchronous IO scheduler.
 *
 * For each blocking action, if the action can be performed immediately, then it
 * is. Otherwise, the thread performing the blocking task is suspended and
 * automatically wakes up when the action completes. The suspend/resume is
 * transparent to the programmer.
 *)

(* Type declarations *)

type file_descr = Unix.file_descr
type sockaddr = Unix.sockaddr
type msg_flag = Unix.msg_flag

type 'a status =
  | Done of 'a
  | Error of exn
  | Waiting of ('a, unit) continuation list

type 'a promise = 'a status ref

effect Async : ('a -> 'b) * 'a -> 'b promise
effect Await : 'a promise -> 'a
effect Yield : unit

effect Accept : file_descr -> (file_descr * sockaddr)
effect Recv : file_descr * bytes * int * int * msg_flag list -> int
effect Send : file_descr * bytes * int * int * msg_flag list -> int
effect Sleep : float -> unit

type state =
  { run_q    : (unit -> unit) Queue.t;
    read_ht  : (file_descr, Lwt_engine.event * (unit -> unit) Lwt_sequence.t) Hashtbl.t;
    write_ht : (file_descr, Lwt_engine.event * (unit -> unit) Lwt_sequence.t) Hashtbl.t; }

(* Wrappers for performing effects *)

let async f v =
  perform (Async (f, v))

let await p =
  perform (Await p)

let yield () =
  perform Yield

let accept fd =
  perform (Accept fd)

let recv fd buf pos len mode =
  perform (Recv (fd, buf, pos, len, mode))

let send fd bus pos len mode =
  perform (Send (fd, bus, pos, len, mode))

let sleep timeout =
  perform (Sleep timeout)

(* IO loop *)

let clean st =
  let clean_ht ht = 
    let fd_list = 
      (* XXX: Use Hashtbl.filter_map_inplace *)
      Hashtbl.fold (fun fd (ev,seq) acc -> 
        if Lwt_sequence.is_empty seq then
          (Lwt_engine.stop_event ev; fd::acc)
        else acc) ht []
    in
    List.iter (fun fd -> Hashtbl.remove ht fd) fd_list
  in
  clean_ht st.read_ht;
  clean_ht st.write_ht

let rec schedule st =
  if Queue.is_empty st.run_q then (* No runnable threads *)
    if Hashtbl.length st.read_ht = 0 &&
       Hashtbl.length st.write_ht = 0 &&
       Lwt_engine.timer_count () = 0 then () (* We are done *)
    else perform_io st
  else (* Still have runnable threads *)
    Queue.pop st.run_q ()

and perform_io st =
  Lwt_engine.iter true;
  (* TODO: Cleanup should be performed laziyly for performance. *)
  clean st;
  schedule st

(* Syscall wrappers *)

type syscall_kind = 
  | Read 
  | Write

external poll_rd : Unix.file_descr -> bool = "lwt_unix_readable"
external poll_wr : Unix.file_descr -> bool = "lwt_unix_writable"

let register_readable fd seq = 
    Lwt_engine.on_readable fd (fun _ -> Lwt_sequence.iter_l (fun f -> f ()) seq)

let register_writable fd seq = 
    Lwt_engine.on_writable fd (fun _ -> Lwt_sequence.iter_l (fun f -> f ()) seq)

let poll_syscall fd kind action =
  try
    if (kind = Read && poll_rd fd) || (kind = Write && poll_wr fd) then
      Some (action ())
    else None
  with
  | Unix.Unix_error((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _)
  | Sys_blocked_io -> None

let dummy = Lwt_sequence.add_r ignore (Lwt_sequence.create ())

let rec block_syscall st fd kind action k =
  let node = ref dummy in
  let ht,register = 
    match kind with
    | Read -> st.read_ht, register_readable
    | Write -> st.write_ht, register_writable
  in
  let seq = 
    try snd @@ Hashtbl.find ht fd
    with Not_found ->
      let seq = Lwt_sequence.create () in
      let ev = register fd seq in
      Hashtbl.add ht fd (ev, seq);
      seq
  in
  node := Lwt_sequence.add_r (fun () ->
    Lwt_sequence.remove !node;
    match poll_syscall fd kind action with
    | Some res -> Queue.push (fun () -> continue k res) st.run_q
    | None -> block_syscall st fd kind action k) seq

let do_syscall st fd kind action k =
  match poll_syscall fd kind action with
  | Some res -> continue k res
  | None -> 
      block_syscall st fd kind action k;
      schedule st

let block_sleep st delay k =
  ignore @@ Lwt_engine.on_timer delay false (fun ev -> 
    Lwt_engine.stop_event ev; 
    Queue.push (continue k) st.run_q)

(* Promises *)

let mk_status () = ref (Waiting [])

let finish st sr v =
  match !sr with
  | Waiting l ->
      sr := Done v;
      List.iter (fun k ->
        Queue.push (fun () -> continue k v) st.run_q) l
  | _ -> failwith "Impossible: finish"

let abort st sr e =
  match !sr with
  | Waiting l ->
      sr := Error e;
      List.iter (fun k ->
        Queue.push (fun () -> discontinue k e) st.run_q) l
  | _ -> failwith "Impossible: abort"

let force st sr k =
  match !sr with
  | Done v -> continue k v
  | Error e -> discontinue k e
  | Waiting l -> 
      sr := Waiting (k::l); 
      schedule st

(* Main handler loop *)

let init () =
  { run_q = Queue.create ();
    read_ht = Hashtbl.create 13;
    write_ht = Hashtbl.create 13 }

let run main =
  let st = init () in
  let rec fork : 'a. state -> 'a promise -> (unit -> 'a) -> unit = fun st sr f ->
    match f () with
    | v -> finish st sr v; schedule st
    | exception e ->
        print_string (Printexc.to_string e);
        abort st sr e;
        schedule st
    | effect Yield  k ->
        Queue.push (continue k) st.run_q;
        schedule st
    | effect (Async (f, v)) k ->
        let sr = mk_status () in
        Queue.push (fun () -> continue k sr) st.run_q;
        fork st sr (fun () -> f v)
    | effect (Await sr) k -> force st sr k
    | effect (Accept fd) k ->
        let action () = Unix.accept fd in
        do_syscall st fd Read action k
    | effect (Recv (fd, buf, pos, len, mode)) k ->
        let action () = Unix.recv fd buf pos len mode in
        do_syscall st fd Read action k
    | effect (Send (fd, buf, pos, len, mode)) k ->
        let action () = Unix.send fd buf pos len mode in
        do_syscall st fd Write action k
    | effect (Sleep t) k ->
        if t <= 0. then continue k ()
        else begin
          block_sleep st t k;
          schedule st
        end
  in
  let sr = mk_status () in
  fork st sr main
