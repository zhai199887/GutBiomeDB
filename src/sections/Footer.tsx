import classes from "./Footer.module.css";

const Footer = () => (
  <footer>
    <p>
      A project of the{" "}
      <strong>Department of Gastroenterology</strong>,{" "}
      <a href="https://www.cmu.edu.cn" target="_blank" rel="noreferrer">
        China Medical University
      </a>
      .
    </p>

    <div className={classes.cols}>
      <a
        href="https://www.cmu.edu.cn"
        target="_blank"
        rel="noreferrer"
        data-tooltip="China Medical University · Department of Gastroenterology"
        className={classes.logoBlock}
      >
        <div className={classes.logoText}>
          <span className={classes.logoEn}>China Medical University</span>
          <span className={classes.logoCn}>中国医科大学</span>
          <span className={classes.logoDept}>Department of Gastroenterology</span>
        </div>
      </a>
    </div>
  </footer>
);

export default Footer;
